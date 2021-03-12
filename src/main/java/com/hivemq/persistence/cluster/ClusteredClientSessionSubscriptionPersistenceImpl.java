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
package com.hivemq.persistence.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.hivemq.cluster.rpc.*;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extensions.iteration.ChunkCursor;
import com.hivemq.extensions.iteration.MultipleChunkResult;
import com.hivemq.mqtt.message.subscribe.Topic;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSessionSubscriptionPersistence;
import com.hivemq.persistence.clientsession.Subscription;
import com.hivemq.persistence.clientsession.callback.SubscriptionResult;
import com.hivemq.persistence.cluster.rpc.Adapters;
import com.hivemq.persistence.cluster.rpc.GRPCChannelRegistry;
import io.grpc.ManagedChannel;
import org.infinispan.commons.api.AsyncCache;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class ClusteredClientSessionSubscriptionPersistenceImpl implements ClientSessionSubscriptionPersistence {
    private AsyncCache<String,String> sessions;
    private final SingleWriterService singleWriterService;
    private final ClientSessionSubscriptionPersistence persistence;
    private final GRPCChannelRegistry channels;

    public ClusteredClientSessionSubscriptionPersistenceImpl(
            @NotNull final SingleWriterService singleWriterService,
            @NotNull final ClientSessionSubscriptionPersistence persistence,
            @NotNull final GRPCChannelRegistry channels) {
        this.singleWriterService = singleWriterService;
        this.persistence = persistence;
        this.channels = channels;
    }

    public void openDB(@NotNull final InfinispanClusteringService service) {
        this.sessions = service.getCache("client.sessions");
    }

    @Override
    public @NotNull ListenableFuture<SubscriptionResult> addSubscription(@NotNull final String client,
                                                                         @NotNull final Topic topic) {
        final ListenableFuture<ImmutableList<SubscriptionResult>> added =
                addSubscriptions(client, ImmutableSet.<Topic>builder().add(topic).build());

        final SettableFuture<SubscriptionResult> result = SettableFuture.create();

        Futures.addCallback(added, new FutureCallback<>() {
            @Override
            public void onSuccess(final ImmutableList<SubscriptionResult> results) {
                result.set(results.get(0));
            }

            @Override
            public void onFailure(Throwable t) {
                result.setException(t);
            }
        }, singleWriterService.callbackExecutor(client));

        return result;
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
        return null;
    }

    @Override
    public @NotNull ListenableFuture<ImmutableList<SubscriptionResult>> addSubscriptions(
            @NotNull final String clientId,
            @NotNull final ImmutableSet<Topic> topics) {
        final CompletableFuture<String> fetched = sessions.getAsync(clientId);

        final CompletableFuture<ImmutableList<SubscriptionResult>> added = fetched.thenCompose(nodeAddress -> {
            if (Objects.nonNull(nodeAddress)) {
                return remoteAddSubscriptions(nodeAddress, clientId, topics);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });

        final SettableFuture<ImmutableList<SubscriptionResult>> converted = SettableFuture.create();

        added.whenComplete((results, th) -> {
            if (Objects.nonNull(th)) {
                converted.setException(th);
            } else {
                converted.set(results);
            }
        });

        return converted;
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

    private CompletableFuture<ImmutableList<SubscriptionResult>> remoteAddSubscriptions(
            @NotNull final String address,
            @NotNull final String clientId,
            @NotNull final ImmutableSet<Topic> topics) {
        final SubscriptionServiceGrpc.SubscriptionServiceFutureStub client = client(address);

        final AddSubscriptionsRequest request = buildAddSubscriptionsRequest(clientId, topics);

        final ListenableFuture<AddSubscriptionsResponse> rpcResult = client.addSubscriptions(request);

        final CompletableFuture<ImmutableList<SubscriptionResult>> added = new CompletableFuture<>();

        Futures.addCallback(
                rpcResult,
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(final AddSubscriptionsResponse response) {
                        final ImmutableList.Builder<SubscriptionResult> results = ImmutableList.builder();

                        for (final SubscriptionResultModel model: response.getResultsList())  {
                            final SubscriptionResult result = new SubscriptionResult(
                                    Adapters.adapt(model.getTopic()), model.getSubscriptionAlreadyExisted(), model.getShareName()
                            );

                            results.add(result);
                        }

                        added.complete(results.build());
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        added.completeExceptionally(t);
                    }
                },
                singleWriterService.callbackExecutor(clientId)
        );

        return added;
    }

    private AddSubscriptionsRequest buildAddSubscriptionsRequest(@NotNull final String client,
            @NotNull final ImmutableSet<Topic> topics) {
        final AddSubscriptionsRequest.Builder builder = AddSubscriptionsRequest.newBuilder();

        builder.setClient(client);

        for (final Topic topic: topics) {
            builder.addTopic(Adapters.adapt(topic));
        }

        return builder.build();
    }

    private SubscriptionServiceGrpc.SubscriptionServiceFutureStub client(@NotNull final String address) {
        //final ManagedChannel channel = channels.get(address);

        //return SubscriptionServiceGrpc.newFutureStub(channel);
        return null;
    }
}
