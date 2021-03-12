package com.hivemq.persistence.cluster.rpc;

import com.hivemq.cluster.rpc.SessionPersistenceServiceGrpc;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.cluster.ClusteringService;
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
    private final SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase persistenceService;
    private Server server;

    @Inject
    public GRPCServer(
            @NotNull final ClusteringService clusteringService,
            @NotNull final SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase persistenceService) {
        this.clusteringService = clusteringService;
        this.persistenceService = persistenceService;
    }

    public void open() throws IOException {
        server = NettyServerBuilder
                .forPort(clusteringService.getRPCPort())
                .addService(persistenceService)
                .build();

        server.start();
    }

    public void close() {
        server.shutdown();
    }
}
