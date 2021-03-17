package com.hivemq.cluster.rpc;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.cluster.ClusteringService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Singleton
public class GRPCServer {
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
    private Server server;

    @Inject
    public GRPCServer(
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

    public void open() throws IOException {
        server = NettyServerBuilder
                .forPort(clusteringService.getRPCPort())
                .addService(sessionPersistenceService)
                .addService(queuePersistenceService)
                .addService(subPersistenceService)
                .addService(publishService)
                .build();

        server.start();
    }

    public void close() {
        server.shutdown();
    }
}
