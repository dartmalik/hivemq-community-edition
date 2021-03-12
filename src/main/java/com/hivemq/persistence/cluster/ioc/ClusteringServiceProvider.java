package com.hivemq.persistence.cluster.ioc;

import com.hivemq.persistence.cluster.ClusteringService;
import com.hivemq.persistence.cluster.InfinispanClusteringService;

import javax.inject.Provider;

public class ClusteringServiceProvider implements Provider<ClusteringService> {
    @Override
    public ClusteringService get() {
        final InfinispanClusteringService service = new InfinispanClusteringService();

        service.open();

        return service;
    }
}
