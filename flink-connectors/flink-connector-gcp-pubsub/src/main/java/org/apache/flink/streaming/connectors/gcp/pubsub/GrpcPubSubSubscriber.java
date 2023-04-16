/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implementation for {@link PubSubSubscriber}. This Grpc PubSubSubscriber allows for flexible
 * timeouts and retries.
 */
public class GrpcPubSubSubscriber implements PubSubSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcPubSubSubscriber.class);
    private final String projectSubscriptionName;
    private final SubscriberStub stub;
    private final PullRequest pullRequest;
    private final int concurrentPullRequests;

    private ExecutorService executor;

    public GrpcPubSubSubscriber(
            String projectSubscriptionName,
            SubscriberStub stub,
            PullRequest pullRequest,
            int concurrentPullRequests) {
        this.projectSubscriptionName = projectSubscriptionName;
        this.stub = stub;
        this.pullRequest = pullRequest;
        this.concurrentPullRequests = concurrentPullRequests;
        this.executor = Executors.newFixedThreadPool(concurrentPullRequests);
    }

    public GrpcPubSubSubscriber(
            String projectSubscriptionName, SubscriberStub stub, PullRequest pullRequest) {
        this.projectSubscriptionName = projectSubscriptionName;
        this.stub = stub;
        this.pullRequest = pullRequest;
        this.concurrentPullRequests = 1;
    }

    @Override
    public List<ReceivedMessage> pull() {
        CountDownLatch latch = new CountDownLatch(concurrentPullRequests);
        List<ReceivedMessage> receivedMessageList = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < concurrentPullRequests; i++) {
            executor.submit(() -> {
                try {
                    PullResponse response = stub.pullCallable().futureCall(pullRequest).get(60L, SECONDS);
                    receivedMessageList.addAll(response.getReceivedMessagesList());
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOG.error("Encountered exception in Pull futures! " + e);
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Encountered InterruptedException while waiting for Pull futures to complete! " + e);
        }

        if (errorCount.get() > 0) {
            // Handle the case where one or more pull requests encountered an exception.
            // You can choose how to handle it, e.g., return an empty list or throw a custom exception.
            LOG.error("Error count was: " + errorCount.get());
        }
        return receivedMessageList;
    }

    @Override
    public void acknowledge(List<String> acknowledgementIds) {
        LOG.info("Acknowledge inside grpcpubsubsubscriber has been called");
        if (acknowledgementIds.isEmpty()) {
            return;
        }

        // grpc servers won't accept acknowledge requests that are too large so we split the ackIds
        List<List<String>> splittedAckIds = splitAckIds(acknowledgementIds);
        LOG.info(
                "Splits: "
                        + splittedAckIds.size()
                        + " and "
                        + splittedAckIds.stream().map(List::size).reduce(0, Integer::sum)
                        + " elements.");
        splittedAckIds.forEach(
                batch ->
                        stub.acknowledgeCallable()
                                .call(
                                        AcknowledgeRequest.newBuilder()
                                                .setSubscription(projectSubscriptionName)
                                                .addAllAckIds(batch)
                                                .build()));
        LOG.info("Finished calling all acknowledgeCallables. End of function.");
    }

    /* maxPayload is the maximum number of bytes to devote to actual ids in
    * acknowledgement or modifyAckDeadline requests. A serialized
    * AcknowledgeRequest grpc call has a small constant overhead, plus the size of the
    * subscription name, plus 3 bytes per ID (a tag byte and two size bytes). A
    * ModifyAckDeadlineRequest has an additional few bytes for the deadline. We
    * don't know the subscription name here, so we just assume the size exclusive
    * of ids is 100 bytes.

    * With gRPC there is no way for the client to know the server's max message size (it is
    * configurable on the server). We know from experience that it is 512K.
    * @return First list contains no more than 512k bytes, second list contains remaining ids
    */
    private List<List<String>> splitAckIds(List<String> ackIds) {
        final int maxPayload = 500 * 1024; // little below 512k bytes to be on the safe side
        final int fixedOverheadPerCall = 100;
        final int overheadPerId = 3;

        List<List<String>> outputLists = new ArrayList<>();
        List<String> currentList = new ArrayList<>();
        int totalBytes = fixedOverheadPerCall;

        for (String ackId : ackIds) {
            if (totalBytes + ackId.length() + overheadPerId > maxPayload) {
                outputLists.add(currentList);
                currentList = new ArrayList<>();
                totalBytes = fixedOverheadPerCall;
            }
            currentList.add(ackId);
            totalBytes += ackId.length() + overheadPerId;
        }
        if (!currentList.isEmpty()) {
            outputLists.add(currentList);
        }

        return outputLists;
    }

    @Override
    public void close() throws Exception {
        stub.shutdownNow();
        stub.awaitTermination(20, SECONDS);
    }
}
