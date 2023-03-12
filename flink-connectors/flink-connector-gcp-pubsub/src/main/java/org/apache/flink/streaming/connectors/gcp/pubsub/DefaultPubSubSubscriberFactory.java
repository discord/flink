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

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.Subscription;
import jdk.internal.net.http.websocket.Transport;
import org.apache.flink.connector.gcp.pubsub.source.PubSubSource;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.PullRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import org.threeten.bp.Duration;
import java.util.Arrays;
import java.util.HashSet;

/**
 * A default {@link PubSubSubscriberFactory} used by the {@link PubSubSource.PubSubSourceBuilder} to
 * obtain a subscriber with which messages can be pulled from GCP Pub/Sub.
 */
public class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
    private final TransportChannelProvider channelProvider;
    private final int retries;
    private final Duration timeout;
    private final int maxMessagesPerPull;
    private final String projectSubscriptionName;

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
            TransportChannelProvider channelProvider,
            String projectSubscriptionName,
            int retries,
            Duration pullTimeout,
            int maxMessagesPerPull) {
        this.channelProvider = channelProvider;
        this.retries = retries;
        this.timeout = pullTimeout;
        this.maxMessagesPerPull = maxMessagesPerPull;
        this.projectSubscriptionName = projectSubscriptionName;
    }

    public DefaultPubSubSubscriberFactory(
            String projectSubscriptionName,
            int retries,
            Duration pullTimeout,
            int maxMessagesPerPull
    ) {
        this(
                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                        .build(),
                projectSubscriptionName,
                retries,
                pullTimeout,
                maxMessagesPerPull
        );
    }

    @Override
    public PubSubSubscriber getSubscriber(Credentials credentials) throws IOException {

        SubscriberStubSettings.Builder subscriberSettingsBuilder = SubscriberStubSettings.newBuilder();

        subscriberSettingsBuilder
                .createSubscriptionSettings()
                .setRetrySettings(
                        subscriberSettingsBuilder
                                .createSubscriptionSettings()
                                .getRetrySettings().toBuilder()
                                .setInitialRetryDelay(Duration.ofMillis(10L))
                                .setInitialRpcTimeout(timeout)
                                .setMaxAttempts(retries)
                                .setMaxRetryDelay(Duration.ofSeconds(10L))
                                .setMaxRpcTimeout(timeout)
                                .setRetryDelayMultiplier(1.4)
                                .build())
                .setRetryableCodes(new HashSet<>(Arrays.asList(
                        StatusCode.Code.UNAVAILABLE,
                        StatusCode.Code.DEADLINE_EXCEEDED)));

        SubscriberStubSettings subscriberSettings = subscriberSettingsBuilder
                .setTransportChannelProvider(channelProvider)
                .build();

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
