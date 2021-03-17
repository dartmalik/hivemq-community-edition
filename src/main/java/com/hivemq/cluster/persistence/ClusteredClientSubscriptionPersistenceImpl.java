/*
 * Copyright 2019-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.cluster.persistence;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.cluster.ClusteringService;
import com.hivemq.cluster.rpc.*;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extensions.iteration.ChunkCursor;
import com.hivemq.extensions.iteration.MultipleChunkResult;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSessionSubscriptionPersistence;
import com.hivemq.persistence.clientsession.ClientSessionSubscriptionPersistenceImpl;
import com.hivemq.persistence.clientsession.Subscription;
import com.hivemq.persistence.clientsession.callback.SubscriptionResult;
import com.hivemq.cluster.rpc.Adapters;
import com.hivemq.cluster.rpc.GRPCChannelRegistry;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@LazySingleton
public class ClusteredClientSubscriptionPersistenceImpl
        extends SubscriptionPersistenceServiceGrpc.SubscriptionPersistenceServiceImplBase
        implements ClientSessionSubscriptionPersistence {
    private final SingleWriterService singleWriterService;
    private final ClientSessionSubscriptionPersistenceImpl persistence;
    private final GRPCChannelRegistry channels;
    private final ClusteringService clusteringService;

    @Inject
    public ClusteredClientSubscriptionPersistenceImpl(
            @NotNull final SingleWriterService singleWriterService,
            @NotNull final ClientSessionSubscriptionPersistenceImpl persistence,
            @NotNull final ClusteringService clusteringService,
            @NotNull final GRPCChannelRegistry channelRegistry) {
        this.singleWriterService = singleWriterService;
        this.persistence = persistence;
        this.clusteringService = clusteringService;
        this.channels = channelRegistry;
    }

    @Override
    public @NotNull ListenableFuture<SubscriptionResult> addSubscription(@NotNull final String client,
                                                                         @NotNull final Topic topic) {
        final ListenableFuture<ImmutableList<SubscriptionResult>> added =
                addSubscriptions(client, ImmutableSet.<Topic>builder().add(topic).build());

        return Futures.transform(
                added,
                subscriptions -> Objects.nonNull(subscriptions) ? subscriptions.get(0) : null,
                singleWriterService.callbackExecutor(client)
        );
    }

    @Override
    public @NotNull ImmutableSet<Topic> getSubscriptions(@NotNull final String client) {
        return persistence.getSubscriptions(client);
    }

    @Override
    public @NotNull ListenableFuture<MultipleChunkResult<Map<String, ImmutableSet<Topic>>>>
                        getAllLocalSubscribersChunk(@NotNull final ChunkCursor cursor) {
        return persistence.getAllLocalSubscribersChunk(cursor);
    }

    @Override
    public @NotNull ListenableFuture<Void> remove(@NotNull final String client, @NotNull final String topic) {
        return persistence.remove(client, topic);
    }

    @Override
    public @NotNull ListenableFuture<Void> closeDB() {
        return persistence.closeDB();
    }

    @Override
    public @NotNull ListenableFuture<ImmutableList<SubscriptionResult>> addSubscriptions(
            @NotNull final String clientId,
            @NotNull final ImmutableSet<Topic> topics) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(clientId);

        final CompletableFuture<AddSubscriptionsResponse> added = nodeFetched.thenCompose(node -> {
            final AddSubscriptionsRequest.Builder requestBuilder = AddSubscriptionsRequest.newBuilder()
                    .setClient(clientId);

            for (final Topic topic: topics) {
                requestBuilder.addTopic(Adapters.adapt(topic));
            }

            return Adapters.adapt(
                    client(node).addSubscriptions(requestBuilder.build()),
                    singleWriterService.callbackExecutor(clientId)
            );
        });

        final CompletableFuture<AddSubscriptionsResponse> registered = added.thenCompose(response -> {
            final CompletableFuture<Void> subscribed = clusteringService.subscribeTopics(
                    clientId, topics.stream().map(Topic::getTopic).collect(Collectors.toSet())
            );

            return subscribed.thenApply(v -> response);
        });

        final CompletableFuture<ImmutableList<SubscriptionResult>> adapted = registered.thenApply(response -> {
            final ImmutableList.Builder<SubscriptionResult> result = new ImmutableList.Builder<>();

            for (final SubscriptionResultModel model: response.getResultsList()) {
                result.add(Adapters.adapt(model));
            }

            return result.build();
        });

        return Adapters.adapt(adapted);
    }

    @Override
    public @NotNull ListenableFuture<Void> removeSubscriptions(@NotNull final String clientId,
                                                               @NotNull final ImmutableSet<String> topics) {
        return persistence.removeSubscriptions(clientId, topics);
    }

    @Override
    public @NotNull ListenableFuture<Void> removeAll(@NotNull final String clientId) {
        return persistence.removeAll(clientId);
    }

    @Override
    public @NotNull ListenableFuture<Void> removeAllLocally(@NotNull final String clientId) {
        return persistence.removeAllLocally(clientId);
    }

    @Override
    public @NotNull ListenableFuture<Void> cleanUp(final int bucketIndex) {
        return persistence.cleanUp(bucketIndex);
    }

    @Override
    public @NotNull ImmutableSet<Topic> getSharedSubscriptions(@NotNull final String client) {
        return persistence.getSharedSubscriptions(client);
    }

    @Override
    public void invalidateSharedSubscriptionCacheAndPoll(@NotNull final String clientId,
                                                         @NotNull final ImmutableSet<Subscription> sharedSubs) {
        persistence.invalidateSharedSubscriptionCacheAndPoll(clientId, sharedSubs);
    }

    @Override
    public void addSubscriptions(
            final AddSubscriptionsRequest request,
            final StreamObserver<AddSubscriptionsResponse> responseObserver) {
        final ImmutableSet.Builder<Topic> builder = ImmutableSet.builder();

        for (final TopicModel model: request.getTopicList()) {
            builder.add(Adapters.adapt(model));
        }

        final ListenableFuture<ImmutableList<SubscriptionResult>> added =
                persistence.addSubscriptions(request.getClient(), builder.build());

        Futures.addCallback(added, new FutureCallback<>() {
            @Override
            public void onSuccess(final ImmutableList<SubscriptionResult> results) {
                final AddSubscriptionsResponse.Builder responseBuilder = AddSubscriptionsResponse.newBuilder();

                for (final SubscriptionResult res: results) {
                    responseBuilder.addResults(Adapters.adapt(res));
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(final Throwable t) {
                responseObserver.onError(t);
            }
        }, singleWriterService.callbackExecutor(request.getClient()));
    }

    @Override
    public void getSubscriptions(final GetSubscriptionsRequest request,
            final StreamObserver<GetSubscriptionsResponse> responseObserver) {
        final ImmutableSet<Topic> topics = persistence.getSubscriptions(request.getClient());
        final GetSubscriptionsResponse.Builder responseBuilder = GetSubscriptionsResponse.newBuilder();

        for (final Topic topic: topics) {
            responseBuilder.addTopics(Adapters.adapt(topic));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void removeSubscriptions(final RemoveSubscriptionsRequest request,
            final StreamObserver<RemoveSubscriptionsResponse> responseObserver) {
        final Set<String> topics = request.getTopicList()
                .stream()
                .map(TopicModel::getTopic)
                .collect(Collectors.toSet());

        final ImmutableSet<String> topicsSet = ImmutableSet.<String>builder()
                .addAll(topics)
                .build();

        final ListenableFuture<Void> removed = persistence.removeSubscriptions(request.getClient(), topicsSet);

        Futures.addCallback(removed, new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                responseObserver.onNext(RemoveSubscriptionsResponse.newBuilder().build());
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                responseObserver.onError(t);
            }
        }, singleWriterService.callbackExecutor(request.getClient()));
    }

    private SubscriptionPersistenceServiceGrpc.SubscriptionPersistenceServiceFutureStub client(
            @NotNull final InetSocketAddress address) {
        final ManagedChannel channel = channels.get(address.getHostName(), address.getPort());

        return SubscriptionPersistenceServiceGrpc.newFutureStub(channel);
    }
}
