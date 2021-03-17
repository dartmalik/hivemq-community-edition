package com.hivemq.cluster.persistence;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.cluster.ClusteringService;
import com.hivemq.cluster.rpc.ForceDisconnectClientRequest;
import com.hivemq.cluster.rpc.ForceDisconnectClientResponse;
import com.hivemq.cluster.rpc.SessionPersistenceServiceGrpc;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extensions.iteration.ChunkCursor;
import com.hivemq.extensions.iteration.MultipleChunkResult;
import com.hivemq.mqtt.message.connect.MqttWillPublish;
import com.hivemq.mqtt.message.reason.Mqtt5DisconnectReasonCode;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSession;
import com.hivemq.persistence.clientsession.ClientSessionPersistence;
import com.hivemq.persistence.clientsession.ClientSessionPersistenceImpl;
import com.hivemq.persistence.clientsession.PendingWillMessages;
import com.hivemq.cluster.rpc.Adapters;
import com.hivemq.cluster.rpc.GRPCChannelRegistry;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@LazySingleton
public class ClusteredClientSessionPersistenceImpl
        extends SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase
        implements ClientSessionPersistence {
    @NotNull
    private final ClientSessionPersistenceImpl localPersistence;
    @NotNull
    private final SingleWriterService singleWriterService;
    @NotNull
    private final ClusteringService clusteringService;
    @NotNull
    private final GRPCChannelRegistry channelRegistry;

    @Inject
    public ClusteredClientSessionPersistenceImpl(
            @NotNull final ClientSessionPersistenceImpl localPersistence,
            @NotNull final SingleWriterService singleWriterService,
            @NotNull final ClusteringService clusteringService,
            @NotNull final GRPCChannelRegistry channelRegistry) {
        this.localPersistence = localPersistence;
        this.singleWriterService = singleWriterService;
        this.clusteringService = clusteringService;
        this.channelRegistry = channelRegistry;
    }

    @Override
    public boolean isExistent(@NotNull final String client) {
        return localPersistence.isExistent(client);
    }

    @Override
    public @NotNull ListenableFuture<Void> clientConnected(
            @NotNull final String client,
            final boolean cleanStart,
            final long sessionExpiryInterval,
            @Nullable final MqttWillPublish willPublish,
            @Nullable final Long queueLimit) {
        final CompletableFuture<Boolean> takenOver = takeover(client);

        final CompletableFuture<Void> connected = takenOver.thenCompose(takeover -> {
            if (!takeover) {
                return CompletableFuture.failedFuture(new IllegalStateException("client_takeover_failed"));
            } else {
                return Adapters.adapt(
                        localPersistence.clientConnected(client, cleanStart, sessionExpiryInterval, willPublish, queueLimit),
                        singleWriterService.callbackExecutor(client)
                );
            }
        });

        return Adapters.adapt(connected);
    }

    @Override
    public @NotNull ListenableFuture<Void> clientDisconnected(
            @NotNull final String client,
            final boolean sendWill,
            final long sessionExpiry) {
        final CompletableFuture<Void> unregistered = clusteringService.unregister(client);

        final CompletableFuture<Void> disconnected = unregistered.thenCompose(
                v -> Adapters.adapt(
                        localPersistence.clientDisconnected(client, sendWill, sessionExpiry),
                        singleWriterService.callbackExecutor(client)
                )
        );

        return Adapters.adapt(disconnected);
    }

    @Override
    public @NotNull ListenableFuture<Boolean> forceDisconnectClient(
            @NotNull final String clientId,
            final boolean preventLwtMessage,
            final ClientSessionPersistenceImpl.@NotNull DisconnectSource source) {
        final CompletableFuture<Void> unregistered = clusteringService.unregister(clientId);

        final CompletableFuture<Boolean> disconnected = unregistered.thenCompose(
                v -> Adapters.adapt(
                        localPersistence.forceDisconnectClient(clientId, preventLwtMessage, source),
                        singleWriterService.callbackExecutor(clientId)
                )
        );

        return Adapters.adapt(disconnected);
    }

    @Override
    public @NotNull ListenableFuture<Boolean> forceDisconnectClient(
            @NotNull final String clientId,
            final boolean preventLwtMessage,
            final ClientSessionPersistenceImpl.@NotNull DisconnectSource source,
            @Nullable final Mqtt5DisconnectReasonCode reasonCode,
            @Nullable final String reasonString) {
        final CompletableFuture<Void> unregistered = clusteringService.unregister(clientId);

        final CompletableFuture<Boolean> disconnected = unregistered.thenCompose(
                v -> Adapters.adapt(
                        localPersistence.forceDisconnectClient(clientId, preventLwtMessage, source, reasonCode, reasonString),
                        singleWriterService.callbackExecutor(clientId)
                )
        );

        return Adapters.adapt(disconnected);
    }

    @Override
    public @NotNull ListenableFuture<Void> closeDB() {
        return localPersistence.closeDB();
    }

    @Override
    public @Nullable ClientSession getSession(@NotNull final String clientId, final boolean includeWill) {
        return localPersistence.getSession(clientId, includeWill);
    }

    @Override
    public @NotNull ListenableFuture<Void> cleanUp(final int bucketIndex) {
        return localPersistence.cleanUp(bucketIndex);
    }

    @Override
    public @NotNull ListenableFuture<Set<String>> getAllClients() {
        return localPersistence.getAllClients();
    }

    @Override
    public @NotNull ListenableFuture<Boolean> setSessionExpiryInterval(@NotNull final String clientId,
                                                                       final long sessionExpiryInterval) {
        return null;
    }

    @Override
    public @Nullable Long getSessionExpiryInterval(@NotNull final String clientId) {
        return localPersistence.getSessionExpiryInterval(clientId);
    }

    @Override
    public @NotNull Map<String, Boolean> isExistent(@NotNull final Set<String> clients) {
        return localPersistence.isExistent(clients);
    }

    @Override
    public @NotNull ListenableFuture<Boolean> invalidateSession(
            @NotNull final String clientId,
            final ClientSessionPersistenceImpl.@NotNull DisconnectSource disconnectSource) {
        return null;
    }

    @Override
    public @NotNull ListenableFuture<Map<String, PendingWillMessages.PendingWill>> pendingWills() {
        return null;
    }

    @Override
    public @NotNull ListenableFuture<Void> removeWill(@NotNull final String clientId) {
        return null;
    }

    @Override
    public @NotNull ListenableFuture<MultipleChunkResult<Map<String, ClientSession>>> getAllLocalClientsChunk(
            @NotNull final ChunkCursor cursor) {
        return null;
    }

    @Override
    public void forceDisconnectClient(
            final ForceDisconnectClientRequest request,
            final StreamObserver<ForceDisconnectClientResponse> responseObserver) {
        final ListenableFuture<Boolean> disconnected = forceDisconnectClient(
                request.getClientId(),
                request.getPreventLwtMessage(),
                ClientSessionPersistenceImpl.DisconnectSource.ofNumber(request.getDisconnectSource())
        );

        Futures.addCallback(
                disconnected,
                new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable final Boolean result) {
                        final ForceDisconnectClientResponse.Builder responseBuilder =
                                ForceDisconnectClientResponse.newBuilder();
                        if (Objects.nonNull(result)) {
                            responseBuilder.setStatus(result);
                        }
                        responseObserver.onNext(responseBuilder.build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        responseObserver.onError(t);
                    }
                },
                singleWriterService.callbackExecutor(request.getClientId())
        );
    }

    private CompletableFuture<Boolean> takeover(final String clientId) {
        final CompletableFuture<InetSocketAddress> nodeFetched = clusteringService.getConnectionNode(clientId);

        final CompletableFuture<Boolean> disconnectedExisting = nodeFetched.thenCompose(node -> {
            if (Objects.isNull(node)) {
                return CompletableFuture.completedFuture(true);
            } else {
                return remoteForceDisconnectClient(node, clientId);
            }
        });

        return disconnectedExisting.thenCompose(status -> {
            if (!status) {
                return CompletableFuture.failedFuture(new IOException("failed_to_disconnect_existing_client"));
            } else {
                return clusteringService.register(clientId);
            }
        });
    }

    private CompletableFuture<Boolean> remoteForceDisconnectClient(@NotNull final InetSocketAddress address,
                                                                   @NotNull final String clientId) {
        final SessionPersistenceServiceGrpc.SessionPersistenceServiceFutureStub client = client(address);

        final ForceDisconnectClientRequest request = ForceDisconnectClientRequest.newBuilder()
                .setClientId(clientId)
                .setPreventLwtMessage(false)
                .setDisconnectSource(ClientSessionPersistenceImpl.DisconnectSource.EXTENSION.getNumber())
                .build();

        final ListenableFuture<ForceDisconnectClientResponse> disconnected =
                client.forceDisconnectClient(request);

        return Adapters.adapt(disconnected, singleWriterService.callbackExecutor(clientId))
                .thenApply(ForceDisconnectClientResponse::getStatus);
    }

    private SessionPersistenceServiceGrpc.SessionPersistenceServiceFutureStub client(
            @NotNull final InetSocketAddress node) {
        final ManagedChannel channel = channelRegistry.get(node.getHostName(), node.getPort());

        return SessionPersistenceServiceGrpc.newFutureStub(channel);
    }
}
