/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub.emulator;

import org.apache.flink.streaming.connectors.gcp.pubsub.GrpcPubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * A convenience PubSubSubscriberFactory that can be used to connect to a PubSub emulator. The
 * PubSub emulators do not support SSL or Credentials and as such this SubscriberStub does not
 * require or provide this.
 */
public class PubSubSubscriberFactoryForEmulator implements PubSubSubscriberFactory {
    private final TransportChannelProvider channelProvider;
    private final String projectSubscriptionName;
    private final int retries;
    private final Duration timeout;
    private final int maxMessagesPerPull;

    public PubSubSubscriberFactoryForEmulator(
            TransportChannelProvider channelProvider,
            String project,
            String subscription,
            int retries,
            Duration timeout,
            int maxMessagesPerPull) {
        this.channelProvider = channelProvider;
        this.retries = retries;
        this.timeout = timeout;
        this.maxMessagesPerPull = maxMessagesPerPull;
        this.projectSubscriptionName = ProjectSubscriptionName.format(project, subscription);
    }

    @Override
    public PubSubSubscriber getSubscriber(Credentials credentials) throws IOException {
        SubscriberStubSettings.Builder subscriberSettingsBuilder =
                SubscriberStubSettings.newBuilder();

        subscriberSettingsBuilder
                .createSubscriptionSettings()
                .setRetrySettings(
                        subscriberSettingsBuilder
                                .createSubscriptionSettings()
                                .getRetrySettings()
                                .toBuilder()
                                .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(10L))
                                .setInitialRpcTimeout(timeout)
                                .setMaxAttempts(retries)
                                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(10L))
                                .setMaxRpcTimeout(timeout)
                                .setRetryDelayMultiplier(1.4)
                                .build())
                .setRetryableCodes(
                        new HashSet<>(
                                Arrays.asList(
                                        StatusCode.Code.UNAVAILABLE,
                                        StatusCode.Code.DEADLINE_EXCEEDED)));

        SubscriberStubSettings subscriberSettings =
                subscriberSettingsBuilder.setTransportChannelProvider(this.channelProvider).build();

        SubscriberStub stub = GrpcSubscriberStub.create(subscriberSettings);

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(maxMessagesPerPull)
                        .setSubscription(projectSubscriptionName)
                        .build();

        return new GrpcPubSubSubscriber(
                projectSubscriptionName, stub, pullRequest, retries, timeout);
    }
}
