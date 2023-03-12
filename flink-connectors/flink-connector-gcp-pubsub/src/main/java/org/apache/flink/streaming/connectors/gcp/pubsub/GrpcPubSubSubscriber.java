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

import com.google.api.gax.rpc.ApiCallContext;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.*;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;

import io.grpc.StatusRuntimeException;

import org.threeten.bp.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implementation for {@link PubSubSubscriber}. This Grpc PubSubSubscriber allows for flexible
 * timeouts and retries.
 */
public class GrpcPubSubSubscriber implements PubSubSubscriber {
    private final String projectSubscriptionName;
    private final SubscriberStub stub;
    private final int retries;
    private final Duration timeout;
    private final PullRequest pullRequest;

    public GrpcPubSubSubscriber(
            String projectSubscriptionName,
            SubscriberStub stub,
            PullRequest pullRequest,
            int retries,
            Duration timeout) {
        this.projectSubscriptionName = projectSubscriptionName;
        this.stub = stub;
        this.retries = retries;
        this.timeout = timeout;
        this.pullRequest = pullRequest;
    }

    @Override
    public List<ReceivedMessage> pull() {
        return pull(retries);
    }

    private List<ReceivedMessage> pull(int retriesRemaining) {
        try {
            return stub.pullCallable().call(pullRequest).getReceivedMessagesList();
        } catch (StatusRuntimeException e) {
            if (retriesRemaining > 0) {
                return pull(retriesRemaining - 1);
            }

            throw e;
        }
    }

    @Override
    public void acknowledge(List<String> acknowledgementIds) {
        if (acknowledgementIds.isEmpty()) {
            return;
        }

        // grpc servers won't accept acknowledge requests that are too large so we split the ackIds
        List<List<String>> splittedAckIds = splitAckIds(acknowledgementIds);
        splittedAckIds.forEach(
                batch ->
                        stub.acknowledgeCallable().call(
                                AcknowledgeRequest.newBuilder()
                                        .setSubscription(projectSubscriptionName)
                                        .addAllAckIds(batch)
                                        .build()));
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
