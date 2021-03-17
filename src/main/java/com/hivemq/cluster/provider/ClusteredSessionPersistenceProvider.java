package com.hivemq.cluster.provider;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.clientsession.ClientSessionPersistence;
import com.hivemq.persistence.clientsession.ClientSessionPersistenceImpl;
import com.hivemq.cluster.persistence.ClusteredClientSessionPersistenceImpl;
import com.hivemq.cluster.ClusteringService;
import com.hivemq.cluster.address.AddressRegistry;
import com.hivemq.cluster.rpc.GRPCChannelRegistry;

import javax.inject.Inject;
import javax.inject.Provider;

public class ClusteredSessionPersistenceProvider implements Provider<ClientSessionPersistence> {
    private final Provider<ClientSessionPersistenceImpl> localPersistenceProvider;
    private final AddressRegistry addressRegistry;
    private final SingleWriterService singleWriterService;
    private final ClusteringService clusteringService;
    private final GRPCChannelRegistry channelRegistry;

    @Inject
    public ClusteredSessionPersistenceProvider(
            final Provider<ClientSessionPersistenceImpl> localPersistenceProvider,
            @NotNull final AddressRegistry addressRegistry,
            @NotNull final SingleWriterService singleWriterService,
            @NotNull final ClusteringService clusteringService,
            @NotNull final GRPCChannelRegistry channelRegistry) {
        this.localPersistenceProvider = localPersistenceProvider;
        this.addressRegistry = addressRegistry;
        this.singleWriterService = singleWriterService;
        this.clusteringService = clusteringService;
        this.channelRegistry = channelRegistry;
    }

    @Override
    public ClientSessionPersistence get() {
        return new ClusteredClientSessionPersistenceImpl(
                localPersistenceProvider.get(), singleWriterService, clusteringService, channelRegistry
        );
    }
}
