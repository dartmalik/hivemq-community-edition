package com.hivemq.persistence.cluster.ioc;

import com.hivemq.cluster.rpc.SessionPersistenceServiceGrpc;
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

    @Inject
    public GRPCServerProvider(
            @NotNull final ClusteringService clusteringService,
            @NotNull final SessionPersistenceServiceGrpc.SessionPersistenceServiceImplBase sessionPersistenceService) {
        this.clusteringService = clusteringService;
        this.sessionPersistenceService = sessionPersistenceService;
    }

    @Override
    public GRPCServer get() {
        try {
            final GRPCServer server = new GRPCServer(clusteringService, sessionPersistenceService);

            server.open();

            return server;
        }
        catch (final IOException ex) {
            return null;
        }
    }
}
