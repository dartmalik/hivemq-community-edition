package com.hivemq.persistence.cluster;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
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
import com.hivemq.persistence.cluster.address.Address;
import com.hivemq.persistence.cluster.address.AddressRegistry;
import com.hivemq.persistence.cluster.address.Record;
import com.hivemq.persistence.cluster.rpc.GRPCChannelRegistry;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
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
    private final AddressRegistry registry;
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
        this.registry = clusteringService.getRegistry();
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
        final CompletableFuture<Collection<Record>> fetchedExisting = registry.get(address(client));

        final CompletableFuture<Boolean> disconnectedExisting = fetchedExisting.thenCompose(records -> {
           if (Objects.isNull(records) || records.size() <= 0) {
               return CompletableFuture.completedFuture(true);
           } else {
               final String node = records.stream().findFirst().get().getContent();
               final String[] parts = node.split(":");
               final InetSocketAddress address = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
               return remoteForceDisconnectClient(address, client);
           }
        });

        final CompletableFuture<Boolean> registeredNew = disconnectedExisting.thenCompose(status -> {
            if (!status) {
                return CompletableFuture.failedFuture(new IOException("failed_to_disconnect_existing_client"));
            } else {
                return register(client);
            }
        });

        final CompletableFuture<Void> connected = registeredNew.thenCompose(registered -> {
            if (!registered) {
                return CompletableFuture.failedFuture(new IllegalStateException("client_registeration_failed"));
            } else {
                return adapt(client, localPersistence.clientConnected(client, cleanStart, sessionExpiryInterval, willPublish, queueLimit));
            }
        });

        return adapt(connected);
    }

    @Override
    public @NotNull ListenableFuture<Void> clientDisconnected(@NotNull final String client,
                                                              final boolean sendWill,
                                                              final long sessionExpiry) {
        final CompletableFuture<Void> unregistered = unregister(client);

        final CompletableFuture<Void> disconnected = unregistered.thenCompose(
                v -> adapt(client, localPersistence.clientDisconnected(client, sendWill, sessionExpiry))
        );

        return adapt(disconnected);
    }

    @Override
    public @NotNull ListenableFuture<Boolean> forceDisconnectClient(
            @NotNull final String clientId,
            final boolean preventLwtMessage,
            final ClientSessionPersistenceImpl.@NotNull DisconnectSource source) {
        final CompletableFuture<Void> unregistered = unregister(clientId);

        final CompletableFuture<Boolean> disconnected = unregistered.thenCompose(
                v -> adapt(clientId, localPersistence.forceDisconnectClient(clientId, preventLwtMessage, source))
        );

        return adapt(disconnected);
    }

    @Override
    public @NotNull ListenableFuture<Boolean> forceDisconnectClient(
            @NotNull final String clientId,
            final boolean preventLwtMessage,
            final ClientSessionPersistenceImpl.@NotNull DisconnectSource source,
            @Nullable final Mqtt5DisconnectReasonCode reasonCode,
            @Nullable final String reasonString) {
        final CompletableFuture<Void> unregistered = unregister(clientId);

        final CompletableFuture<Boolean> disconnected = unregistered.thenCompose(
                v -> adapt(
                        clientId,
                        localPersistence.forceDisconnectClient(clientId, preventLwtMessage, source, reasonCode, reasonString)
                )
        );

        return adapt(disconnected);
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
                    public void onSuccess(final Boolean result) {
                        try {
                            final ForceDisconnectClientResponse.Builder responseBuilder =
                                    ForceDisconnectClientResponse.newBuilder();
                            if (Objects.nonNull(result)) {
                                responseBuilder.setStatus(result);
                            }
                            responseObserver.onNext(responseBuilder.build());
                            responseObserver.onCompleted();
                        }
                        catch (Throwable th) {
                            responseObserver.onCompleted();
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        responseObserver.onError(t);
                    }
                },
                singleWriterService.callbackExecutor(request.getClientId())
        );
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

        return adapt(clientId, disconnected)
                .thenApply(ForceDisconnectClientResponse::getStatus);
    }

    private CompletableFuture<Boolean> register(@NotNull final String clientId) {
        final Record record = new Record(clientId, clusteringService.getRPCServiceAddress());

        return registry.put(address(clientId), record, 3600, false);
    }

    private CompletableFuture<Void> unregister(@NotNull final String clientId) {
        final Record record = new Record(clientId, clusteringService.getRPCServiceAddress());

        return registry.remove(address(clientId), record);
    }

    private SessionPersistenceServiceGrpc.SessionPersistenceServiceFutureStub client(
            @NotNull final InetSocketAddress node) {
        final ManagedChannel channel = channelRegistry.get(node.getHostName(), node.getPort());

        return SessionPersistenceServiceGrpc.newFutureStub(channel);
    }

    private Address address(@NotNull final String clientId) {
        return new Address(AddressRegistry.DOMAIN_SESSIONS + clientId);
    }

    private <T> CompletableFuture<T> adapt(final String client, @NotNull final ListenableFuture<T> future) {
        final CompletableFuture<T> result = new CompletableFuture<>();

        Futures.addCallback(
                future,
                new FutureCallback<T>() {
                    @Override
                    public void onSuccess(final T v) {
                        result.complete(v);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        result.completeExceptionally(t);
                    }
                },
                singleWriterService.callbackExecutor(client)
        );

        return result;
    }

    private <T> ListenableFuture<T> adapt(@NotNull final CompletableFuture<T> future) {
        final SettableFuture<T> result = SettableFuture.create();

        future.whenComplete((v, ex) -> {
            if (Objects.nonNull(ex)) {
                result.setException(ex);
            } else {
                result.set(v);
            }
        });

        return result;
    }
}
