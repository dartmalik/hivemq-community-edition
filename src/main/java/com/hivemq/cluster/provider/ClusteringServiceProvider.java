package com.hivemq.cluster.provider;

import com.hivemq.cluster.ClusteringService;
import com.hivemq.cluster.InfinispanClusteringService;

import javax.inject.Provider;

public class ClusteringServiceProvider implements Provider<ClusteringService> {
    @Override
    public ClusteringService get() {
        final InfinispanClusteringService service = new InfinispanClusteringService();

        service.open();

        return service;
    }
}
