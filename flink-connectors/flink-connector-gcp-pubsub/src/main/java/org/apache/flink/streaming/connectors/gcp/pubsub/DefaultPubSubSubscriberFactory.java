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

import org.apache.flink.connector.gcp.pubsub.source.PubSubSource;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.PullRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

/**
 * A default {@link PubSubSubscriberFactory} used by the {@link PubSubSource.PubSubSourceBuilder} to
 * obtain a subscriber with which messages can be pulled from GCP Pub/Sub.
 */
public class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
    private final String hostAndPort;
    private final int retries;
    private final Duration timeout;
    private final int maxMessagesPerPull;
    private final String projectSubscriptionName;
    private final int parallelPullRequests;

    /**
     * @param projectSubscriptionName The formatted name of the Pub/Sub project and subscription to
     *     pull messages from. Can be easily obtained through {@link
     *     com.google.pubsub.v1.ProjectSubscriptionName}.
     * @param retries The number of times the reception of a message should be retried in case of
     *     failure.
     * @param pullTimeout The timeout after which a message pull request is deemed a failure
     * @param maxMessagesPerPull The maximum number of messages that should be pulled in one go.
     */
    public DefaultPubSubSubscriberFactory(
            String hostAndPort,
            String projectSubscriptionName,
            int retries,
            Duration pullTimeout,
            int maxMessagesPerPull,
            int parallelPullRequests) {
        this.hostAndPort = hostAndPort;
        this.retries = retries;
        this.timeout = pullTimeout;
        this.maxMessagesPerPull = maxMessagesPerPull;
        this.projectSubscriptionName = projectSubscriptionName;
        this.parallelPullRequests = parallelPullRequests;
    }

    public DefaultPubSubSubscriberFactory(
            String projectSubscriptionName,
            int retries,
            Duration pullTimeout,
            int maxMessagesPerPull,
            int parallelPullRequests) {
        this(
                null,
                projectSubscriptionName,
                retries,
                pullTimeout,
                maxMessagesPerPull,
                parallelPullRequests);
    }

    public DefaultPubSubSubscriberFactory(
            String projectSubscriptionName,
            int retries,
            Duration pullTimeout,
            int maxMessagesPerPull) {
        this(projectSubscriptionName, retries, pullTimeout, maxMessagesPerPull, 1);
    }

    @Override
    public PubSubSubscriber getSubscriber(Credentials credentials) throws IOException {
        TransportChannelProvider channelProvider;
        if (hostAndPort != null) {
            // We are in testing mode, so create a local channel.
            ManagedChannel managedChannel =
                    NettyChannelBuilder.forTarget(hostAndPort)
                            .usePlaintext() // This is okay because it's only used for testing.
                            .build();

            channelProvider =
                    FixedTransportChannelProvider.create(
                            GrpcTransportChannel.create(managedChannel));
        } else {
            channelProvider =
                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                            .setMaxInboundMessageSize(
                                    20 * 1024 * 1024) // 20MB (maximum message size).
                            .build();
        }

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
                                .setInitialRpcTimeout(
                                        org.threeten.bp.Duration.ofSeconds(
                                                timeout.getSeconds(), timeout.getNano()))
                                .setMaxAttempts(retries)
                                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(10L))
                                .setMaxRpcTimeout(
                                        org.threeten.bp.Duration.ofSeconds(
                                                timeout.getSeconds(), timeout.getNano()))
                                .setRetryDelayMultiplier(1.4)
                                .build())
                .setRetryableCodes(
                        new HashSet<>(
                                Arrays.asList(
                                        StatusCode.Code.UNAVAILABLE,
                                        StatusCode.Code.DEADLINE_EXCEEDED)));

        SubscriberStubSettings subscriberSettings =
                subscriberSettingsBuilder.setTransportChannelProvider(channelProvider).build();

        SubscriberStub stub = GrpcSubscriberStub.create(subscriberSettings);

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(maxMessagesPerPull)
                        .setSubscription(projectSubscriptionName)
                        .build();

        return new GrpcPubSubSubscriber(
                projectSubscriptionName, stub, pullRequest, parallelPullRequests);
    }
}
