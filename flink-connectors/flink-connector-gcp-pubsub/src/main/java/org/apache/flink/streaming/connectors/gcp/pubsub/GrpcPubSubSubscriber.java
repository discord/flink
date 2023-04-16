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

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

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
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private BlockingQueue<ReceivedMessage> messageQueue;

    public GrpcPubSubSubscriber(
            String projectSubscriptionName,
            SubscriberStub stub,
            PullRequest pullRequest,
            int concurrentPullRequests,
            int messageQueueSize) {
        this.projectSubscriptionName = projectSubscriptionName;
        this.stub = stub;
        this.pullRequest = pullRequest;
        this.concurrentPullRequests = concurrentPullRequests;
        this.executor = Executors.newFixedThreadPool(concurrentPullRequests);
        this.messageQueue = new LinkedBlockingQueue<>(messageQueueSize);

        // Continuously pull messages using multiple threads
        for (int i = 0; i < concurrentPullRequests; i++) {
            executor.submit(
                    () -> {
                        while (!Thread.currentThread().isInterrupted()) {
                            List<ReceivedMessage> receivedMessages = null;

                            if (messageQueue.remainingCapacity() <= 500) {
                                try {
                                    SECONDS.sleep(1);
                                } catch (InterruptedException e) {
                                    LOG.error("Encountered exception while sleeping: " + e);
                                }
                                continue;
                            }

                            try {
                                PullResponse response = stub.pullCallable().call(pullRequest);
                                receivedMessages = response.getReceivedMessagesList();
                            } catch (Exception e) {
                                LOG.error("Encountered exception while pulling messages: " + e);
                            }
                            if (receivedMessages == null || receivedMessages.isEmpty()) {
                                try {
                                    SECONDS.sleep(1);
                                } catch (InterruptedException e) {
                                    LOG.error("Encountered exception while sleeping: " + e);
                                }
                                continue;
                            }

                            List<String> messagesToNack = new ArrayList<>();
                            receivedMessages.forEach(
                                    receivedMessage -> {
                                        if (!messageQueue.offer(receivedMessage)) {
                                            messagesToNack.add(receivedMessage.getAckId());
                                        }
                                    });
                            if (!messagesToNack.isEmpty()) {
                                long randSeconds = Math.round(Math.random() * 30d);
                                LOG.warn(
                                        "Message queue is full. Dropping "
                                                + messagesToNack.size()
                                                + " messages, and sleeping for "
                                                + randSeconds
                                                + " seconds.");

                                List<List<String>> splittedAckIds = splitAckIds(messagesToNack);
                                splittedAckIds.forEach(
                                        ackIds -> {
                                            Runnable nackTask =
                                                    () -> {
                                                        stub.modifyAckDeadlineCallable()
                                                                .call(
                                                                        nackMessages(
                                                                                projectSubscriptionName,
                                                                                ackIds));
                                                    };

                                            // Submit the task and get a Future, abd schedule a task
                                            // to cancel the future after 20 seconds
                                            Future<?> nackFuture = scheduler.submit(nackTask);
                                            scheduler.schedule(
                                                    () -> nackFuture.cancel(true), 20, SECONDS);
                                        });

                                try {
                                    SECONDS.sleep(randSeconds);
                                } catch (InterruptedException e) {
                                    LOG.error(
                                            "Encountered exception while sleeping after nacking: "
                                                    + e);
                                }
                            }
                        }
                    });
        }
    }

    public GrpcPubSubSubscriber(
            String projectSubscriptionName, SubscriberStub stub, PullRequest pullRequest) {
        this(projectSubscriptionName, stub, pullRequest, 1, 1000);
    }

    @Override
    public List<ReceivedMessage> pull() {
        List<ReceivedMessage> receivedMessageList = new ArrayList<>();

        // Drain all available messages from the queue
        messageQueue.drainTo(receivedMessageList);
        return receivedMessageList;
    }

    @Override
    public void acknowledge(List<String> acknowledgementIds) {
        if (acknowledgementIds.isEmpty()) {
            LOG.info("No messages to acknowledge!");
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

        // Split ackIds & acknowledge in parallel
        List<CompletableFuture<Void>> list = new ArrayList<>();
        for (List<String> batch : splittedAckIds) {
            CompletableFuture<Void> voidCompletableFuture =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    stub.acknowledgeCallable()
                                            .call(ackMessages(projectSubscriptionName, batch));
                                } catch (Exception e) {
                                    LOG.error(
                                            "Encountered exception while acknowledging messages: "
                                                    + e);
                                }
                            },
                            executor);
            list.add(voidCompletableFuture);
        }
        CompletableFuture<Void> futuresAll =
                CompletableFuture.allOf(list.toArray(new CompletableFuture<?>[0]));

        try {
            futuresAll.get(20, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Encountered exception while acknowledging messages: " + e);
        }

        LOG.info("Finished all acknowledgeCallables. End of function.");
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

    public AcknowledgeRequest ackMessages(String subscription, List<String> ackIds) {
        return AcknowledgeRequest.newBuilder()
                .setSubscription(subscription)
                .addAllAckIds(ackIds)
                .build();
    }

    public ModifyAckDeadlineRequest nackMessages(String subscription, List<String> ackIds) {
        return ModifyAckDeadlineRequest.newBuilder()
                .setSubscription(subscription)
                .addAllAckIds(ackIds)
                .setAckDeadlineSeconds(0)
                .build();
    }
}
