package com.hivemq.persistence.cluster.ioc;

import com.hivemq.cluster.rpc.PublishServiceGrpc;
import com.hivemq.cluster.rpc.QueuePersistenceServiceGrpc;
import com.hivemq.cluster.rpc.SessionPersistenceServiceGrpc;
import com.hivemq.cluster.rpc.SubscriptionPersistenceServiceGrpc;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.cluster.ClusteringService;
import com.hivemq.persistence.cluster.rpc.GRPCServer;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;

public class GRPCServerProvider implements Provider<GRPCServer> {
    @NotNull
    private final ClusteringService clusteringService;
    @NotNull
    private final SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase sessionPersistenceService;
    @NotNull
    private final QueuePersistenceServiceGrpc.QueuePersistenceServiceImplBase queuePersistenceService;
    @NotNull
    private final SubscriptionPersistenceServiceGrpc.SubscriptionPersistenceServiceImplBase subPersistenceService;
    @NotNull
    private final PublishServiceGrpc.PublishServiceImplBase publishService;

    @Inject
    public GRPCServerProvider(
            @NotNull final ClusteringService clusteringService,
            @NotNull final SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase sessionPersistenceService,
            @NotNull final QueuePersistenceServiceGrpc.QueuePersistenceServiceImplBase queuePersistenceService,
            @NotNull final SubscriptionPersistenceServiceGrpc.SubscriptionPersistenceServiceImplBase subPersistenceService,
            @NotNull final PublishServiceGrpc.PublishServiceImplBase publishService) {
        this.clusteringService = clusteringService;
        this.sessionPersistenceService = sessionPersistenceService;
        this.queuePersistenceService = queuePersistenceService;
        this.subPersistenceService = subPersistenceService;
        this.publishService = publishService;
    }

    @Override
    public GRPCServer get() {
        try {
            final GRPCServer server = new GRPCServer(
                    clusteringService,
                    sessionPersistenceService,
                    queuePersistenceService,
                    subPersistenceService,
                    publishService
            );

            server.open();

            return server;
        }
        catch (final IOException ex) {
            return null;
        }
    }
}
