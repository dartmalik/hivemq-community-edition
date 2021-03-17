package com.hivemq.cluster.persistence;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.ImmutableIntArray;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.cluster.ClusteringService;
import com.hivemq.cluster.rpc.*;
import com.hivemq.configuration.service.MqttConfigurationService;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.MessageWithID;
import com.hivemq.mqtt.message.dropping.MessageDroppedService;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.mqtt.services.PublishPollService;
import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import com.hivemq.persistence.ChannelPersistence;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientqueue.ClientQueueLocalPersistence;
import com.hivemq.persistence.clientqueue.ClientQueuePersistenceImpl;
import com.hivemq.cluster.rpc.Adapters;
import com.hivemq.cluster.rpc.GRPCChannelRegistry;
import com.hivemq.persistence.local.ClientSessionLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@SuppressWarnings("UnstableApiUsage")
@LazySingleton
public class ClusteredClientQueuePersistenceImpl extends ClientQueuePersistenceImpl {
    @LazySingleton
    public static class QueuePersistenceServiceImpl extends QueuePersistenceServiceGrpc.QueuePersistenceServiceImplBase {
        @NotNull
        private final ClientQueuePersistenceImpl localQueuePersistence;
        @NotNull
        private final PublishPayloadPersistence payloadPersistence;
        @NotNull
        private final SingleWriterService singleWriterService;

        @Inject
        public QueuePersistenceServiceImpl(
                @NotNull final ClientQueuePersistenceImpl localQueuePersistence,
                @NotNull final PublishPayloadPersistence payloadPersistence,
                @NotNull final SingleWriterService singleWriterService) {
            this.localQueuePersistence = localQueuePersistence;
            this.payloadPersistence = payloadPersistence;
            this.singleWriterService = singleWriterService;
        }

        @Override
        public void add(final AddMessageRequest request, final StreamObserver<AddMessageResponse> responseObserver) {
            final String queueId = request.getQueueId();
            final boolean shared = request.getShared();
            final boolean retained = request.getRetained();
            final long queueLimit = request.getQueueLimit();

            final List<PUBLISH> publishes = new ArrayList<>();
            for (final PublishModel model: request.getPublishedList()) {
                publishes.add(Adapters.adapt(model, payloadPersistence));
            }

            final CompletableFuture<Integer> sizeFetched = localSize(queueId, shared);

            final CompletableFuture<Integer> added = sizeFetched.thenCompose(
                    size -> localAdd(queueId, shared, publishes, retained, queueLimit)
                            .thenApply(v -> size)
            );

            added.whenComplete((size, ex) -> {
                if (Objects.nonNull(ex)) {
                    responseObserver.onError(ex);
                } else {
                    responseObserver.onNext(AddMessageResponse.newBuilder().setSize(size+1).build());
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void readNew(final ReadNewRequest request, final StreamObserver<ReadNewResponse> responseObserver) {
            final String queueId = request.getQueueId();
            final boolean shared = request.getShared();
            final long byteLimit = request.getByteLimit();
            final ImmutableIntArray packetIds = ImmutableIntArray.builder()
                    .addAll(request.getPacketIdsList())
                    .build();

            final CompletableFuture<ImmutableList<PUBLISH>> read =
                    adapt(queueId, localQueuePersistence.readNew(queueId, shared, packetIds, byteLimit));

            read.whenComplete((publishes, ex) -> {
               if (Objects.isNull(ex)) {
                   final ReadNewResponse.Builder responseBuilder = ReadNewResponse.newBuilder();
                   for (final PUBLISH publish: publishes) {
                       responseBuilder.addPublishes(Adapters.adapt(publish));
                   }
                   responseObserver.onNext(responseBuilder.build());
                   responseObserver.onCompleted();
               } else {
                   responseObserver.onError(ex);
               }
            });
        }

        @Override
        public void readInflight(
                final ReadInflightRequest request, final StreamObserver<ReadInflightResponse> responseObserver) {
            final String clientId = request.getClient();
            final long byteLimit = request.getByteLimit();
            final int messageLimit = request.getMessageLimit();
            final CompletableFuture<ImmutableList<MessageWithID>> read =
                    adapt(clientId, localQueuePersistence.readInflight(clientId, byteLimit, messageLimit));

            read.whenComplete((messages, ex) -> {
               if (Objects.isNull(ex)) {
                   final ReadInflightResponse.Builder builder = ReadInflightResponse.newBuilder();

                   for (final MessageWithID message: messages) {
                       builder.addMessages(Adapters.adapt(message));
                   }

                   responseObserver.onNext(builder.build());
                   responseObserver.onCompleted();
               } else {
                   responseObserver.onError(ex);
               }
            });
        }

        @Override
        public void size(final SizeRequest request, final StreamObserver<SizeResponse> responseObserver) {
            final String queueId = request.getQueueId();
            final boolean shared = request.getShared();

            final CompletableFuture<Integer> fetched = adapt(queueId, localQueuePersistence.size(queueId, shared));

            fetched.whenComplete((size, ex) -> {
               if (Objects.isNull(ex)) {
                   responseObserver.onNext(SizeResponse.newBuilder().setSize(Objects.nonNull(size) ? size : 0).build());
                   responseObserver.onCompleted();
               } else {
                   responseObserver.onError(ex);
               }
            });
        }

        @Override
        public void removeAllQos0Messages(
                final RemoveAllQos0MessagesRequest request,
                final StreamObserver<RemoveAllQos0MessagesResponse> responseObserver) {
            final String queueId = request.getQueueId();
            final boolean shared = request.getShared();
            final CompletableFuture<Void> removed =
                    adapt(queueId, localQueuePersistence.removeAllQos0Messages(queueId, shared));

            removed.whenComplete((v, ex) -> {
               if (Objects.isNull(ex)) {
                   responseObserver.onNext(RemoveAllQos0MessagesResponse.newBuilder().build());
                   responseObserver.onCompleted();
               } else {
                   responseObserver.onError(ex);
               }
            });
        }

        @Override
        public void publishAvailable(
                final PublishAvailableRequest request,
                final StreamObserver<PublishAvailableResponse> responseObserver) {
            log.debug("grpc:publishAvailable");

            localQueuePersistence.publishAvailable(request.getClientId());

            responseObserver.onNext(PublishAvailableResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void remove(final RemoveRequest request, final StreamObserver<RemoveResponse> responseObserver) {
            final String clientId = request.getClientId();
            final int packetId = request.getPacketId();
            final CompletableFuture<Void> removed = adapt(clientId, localQueuePersistence.remove(clientId, packetId));

            removed.whenComplete((v, ex) -> {
                if (Objects.isNull(ex)) {
                    responseObserver.onNext(RemoveResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(ex);
                }
            });
        }

        @Override
        public void putPubrel(final PutPubrelRequest request, final StreamObserver<PutPubrelResponse> responseObserver) {
            final String clientId = request.getClientId();
            final int packetId = request.getPacketId();
            final CompletableFuture<Void> put = adapt(clientId, localQueuePersistence.putPubrel(clientId, packetId));

            put.whenComplete((v, ex) -> {
                if (Objects.isNull(ex)) {
                    responseObserver.onNext(PutPubrelResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(ex);
                }
            });
        }

        @Override
        public void clear(final ClearRequest request, final StreamObserver<ClearResponse> responseObserver) {
            final String queueId = request.getQueueId();
            final boolean shared = request.getShared();
            final CompletableFuture<Void> cleared = adapt(queueId, localQueuePersistence.clear(queueId, shared));

            cleared.whenComplete((v, ex) -> {
                if (Objects.isNull(ex)) {
                    responseObserver.onNext(ClearResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(ex);
                }
            });
        }

        private CompletableFuture<Integer> localSize(final String queueId, final boolean shared) {
            return Adapters.adapt(
                    localQueuePersistence.size(queueId, shared), singleWriterService.callbackExecutor(queueId)
            );
        }

        private CompletableFuture<Void> localAdd(
                final String queueId,
                final boolean shared,
                final List<PUBLISH> publishes,
                final boolean retained,
                final long queueLimit) {
            return Adapters.adapt(
                    localQueuePersistence.add(queueId, shared, publishes, retained, queueLimit),
                    singleWriterService.callbackExecutor(queueId)
            );
        }

        private ExecutorService executor(final String key) {
            return singleWriterService.callbackExecutor(key);
        }

        private <T> CompletableFuture<T> adapt(final String key, final ListenableFuture<T> future) {
            return Adapters.adapt(future, executor(key));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClusteredClientQueuePersistenceImpl.class);

    @NotNull
    private final PublishPayloadPersistence payloadPersistence;
    @NotNull
    private final SingleWriterService singleWriterService;
    @NotNull
    private final ClusteringService clusteringService;
    @NotNull
    private final GRPCChannelRegistry channelRegistry;

    @Inject
    public ClusteredClientQueuePersistenceImpl(
            @NotNull final ClientQueueLocalPersistence localPersistence,
            @NotNull final SingleWriterService singleWriterService,
            @NotNull final MqttConfigurationService mqttConfigurationService,
            @NotNull final ClientSessionLocalPersistence clientSessionLocalPersistence,
            @NotNull final MessageDroppedService messageDroppedService,
            @NotNull final LocalTopicTree topicTree,
            @NotNull final ChannelPersistence channelPersistence,
            @NotNull final PublishPollService publishPollService,
            @NotNull final PublishPayloadPersistence payloadPersistence,
            @NotNull final ClusteringService clusteringService,
            @NotNull final GRPCChannelRegistry channelRegistry) {
        super(
                localPersistence,
                singleWriterService,
                mqttConfigurationService,
                clientSessionLocalPersistence,
                messageDroppedService,
                topicTree,
                channelPersistence,
                publishPollService);
        this.payloadPersistence = payloadPersistence;
        this.singleWriterService = singleWriterService;
        this.clusteringService = clusteringService;
        this.channelRegistry = channelRegistry;
    }

    @Override
    public @NotNull ListenableFuture<Void> add(
            @NotNull final String queueId,
            final boolean shared,
            @NotNull final PUBLISH publish,
            final boolean retained,
            final long queueLimit) {
        return add(queueId, shared, Collections.singletonList(publish), retained, queueLimit);
    }

    @Override
    public @NotNull ListenableFuture<Void> add(
            @NotNull final String queueId,
            final boolean shared,
            @NotNull final List<PUBLISH> publishes,
            final boolean retained,
            final long queueLimit) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(queueId);

        final CompletableFuture<AddMessageResponse> added = nodeFetched.thenCompose(node -> {
            final AddMessageRequest.Builder requestBuilder = AddMessageRequest.newBuilder()
                    .setQueueId(queueId)
                    .setShared(shared)
                    .setRetained(retained)
                    .setQueueLimit(queueLimit);

            for (final PUBLISH publish: publishes) {
                requestBuilder.addPublished(Adapters.adapt(publish));
            }

            return Adapters.adapt(
                    client(node).add(requestBuilder.build()),
                    singleWriterService.callbackExecutor(queueId)
            );
        });

        final CompletableFuture<Void> notified = added.thenCompose(response -> {
            if (response.getSize() == 1) {
                return notifyPublish(queueId);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });

        return Adapters.adapt(notified);
    }

    private CompletableFuture<Void> notifyPublish(@NotNull final String clientId) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getConnectionNode(clientId);

        return nodeFetched.thenCompose(node -> {
            if (Objects.isNull(node)) {
                return CompletableFuture.completedFuture(null);
            }

            final PublishAvailableRequest request = PublishAvailableRequest.newBuilder()
                    .setClientId(clientId)
                    .build();

            return Adapters.adapt(client(node).publishAvailable(request), singleWriterService.callbackExecutor(clientId))
                    .thenCompose(response -> null);
        });
    }

    @Override
    public @NotNull ListenableFuture<ImmutableList<PUBLISH>> readNew(
            @NotNull final String queueId,
            final boolean shared,
            @NotNull final ImmutableIntArray packetIds,
            final long byteLimit) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(queueId);

        final CompletableFuture<ReadNewResponse> read = nodeFetched.thenCompose(node -> {
                final ReadNewRequest.Builder requestBuilder = ReadNewRequest.newBuilder()
                    .setQueueId(queueId)
                    .setShared(shared)
                    .setByteLimit(byteLimit);

            for (final Integer id: packetIds.asList()) {
                requestBuilder.addPacketIds(id);
            }

            return Adapters.adapt(
                    client(node).readNew(requestBuilder.build()),
                    singleWriterService.callbackExecutor(queueId)
            );
        });

        final CompletableFuture<ImmutableList<PUBLISH>> adapted = read.thenApply(response -> {
            final ImmutableList.Builder<PUBLISH> publishes = new ImmutableList.Builder<>();

            for (int pi = 0; pi < response.getPublishesCount(); pi++) {
                publishes.add(Adapters.adapt(response.getPublishes(pi), payloadPersistence));
            }

            return publishes.build();
        });

        return Adapters.adapt(adapted);
    }

    @Override
    public @NotNull ListenableFuture<ImmutableList<MessageWithID>> readInflight(
            @NotNull final String client,
            final long byteLimit,
            final int messageLimit) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(client);

        final CompletableFuture<ReadInflightResponse> read = nodeFetched.thenCompose(node -> {
           final ReadInflightRequest request = ReadInflightRequest.newBuilder()
                   .setClient(client)
                   .setByteLimit(byteLimit)
                   .setMessageLimit(messageLimit)
                   .build();

           return Adapters.adapt(client(node).readInflight(request), singleWriterService.callbackExecutor(client));
        });

        final CompletableFuture<ImmutableList<MessageWithID>> adapted = read.thenApply(response -> {
            final ImmutableList.Builder<MessageWithID> builder = new ImmutableList.Builder<>();

            for (int mi = 0; mi < response.getMessagesCount(); mi++) {
                builder.add(Adapters.adapt(response.getMessages(mi), payloadPersistence));
            }

            return builder.build();
        });

        return Adapters.adapt(adapted);
    }

    @Override
    public @NotNull ListenableFuture<Void> remove(@NotNull final String client, final int packetId) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(client);

        final CompletableFuture<Void> removed = nodeFetched.thenCompose(node -> {
            final RemoveRequest request = RemoveRequest.newBuilder()
                    .setClientId(client)
                    .setPacketId(packetId)
                    .build();

            return Adapters.adapt(client(node).remove(request), singleWriterService.callbackExecutor(client))
                    .thenApply(response -> null);
        });

        return Adapters.adapt(removed);
    }

    @Override
    public @NotNull ListenableFuture<Void> putPubrel(@NotNull final String client, final int packetId) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(client);

        final CompletableFuture<Void> put = nodeFetched.thenCompose(node -> {
            final PutPubrelRequest request = PutPubrelRequest.newBuilder()
                    .setClientId(client)
                    .setPacketId(packetId)
                    .build();

            return Adapters.adapt(client(node).putPubrel(request), singleWriterService.callbackExecutor(client))
                    .thenApply(response -> null);
        });

        return Adapters.adapt(put);
    }

    @Override
    public @NotNull ListenableFuture<Void> clear(@NotNull final String queueId, final boolean shared) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(queueId);

        final CompletableFuture<Void> cleared = nodeFetched.thenCompose(node -> {
            final ClearRequest request = ClearRequest.newBuilder()
                    .setQueueId(queueId)
                    .setShared(shared)
                    .build();

            return Adapters.adapt(client(node).clear(request), singleWriterService.callbackExecutor(queueId))
                    .thenApply(response -> null);
        });

        return Adapters.adapt(cleared);
    }

    @Override
    public @NotNull ListenableFuture<Integer> size(
            @NotNull final String queueId, final boolean shared) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(queueId);

        final CompletableFuture<SizeResponse> fetched = nodeFetched.thenCompose(node -> {
            final SizeRequest request = SizeRequest.newBuilder()
                    .setQueueId(queueId)
                    .setShared(shared)
                    .build();

            return Adapters.adapt(client(node).size(request), singleWriterService.callbackExecutor(queueId));
        });

        return Adapters.adapt(
                fetched.thenApply(SizeResponse::getSize)
        );
    }

    @Override
    public @NotNull ListenableFuture<Void> removeAllQos0Messages(
            @NotNull final String queueId, final boolean shared) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(queueId);

        final CompletableFuture<Void> removed = nodeFetched.thenCompose(node -> {
            final RemoveAllQos0MessagesRequest request = RemoveAllQos0MessagesRequest.newBuilder()
                    .setQueueId(queueId)
                    .setShared(shared)
                    .build();

            return Adapters.adapt(
                    client(node).removeAllQos0Messages(request), singleWriterService.callbackExecutor(queueId)
            ).thenApply(response -> null);
        });

        return Adapters.adapt(removed);
    }

    @Override
    public void publishAvailable(@NotNull final String client) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getOrRegisterSession(client);

        final CompletableFuture<PublishAvailableResponse> sent = nodeFetched.thenCompose(node -> {
            final PublishAvailableRequest request = PublishAvailableRequest.newBuilder()
                    .setClientId(client)
                    .build();

            return Adapters.adapt(client(node).publishAvailable(request), singleWriterService.callbackExecutor(client));
        });

        sent.whenComplete((v, ex) -> {
            if (Objects.nonNull(ex)) {
                log.error(ex.getMessage());
            }
        });
    }

    private QueuePersistenceServiceGrpc.QueuePersistenceServiceFutureStub client(final InetSocketAddress node) {
        final ManagedChannel channel = channelRegistry.get(node.getHostName(), node.getPort());

        return QueuePersistenceServiceGrpc.newFutureStub(channel);
    }
}
