package com.hivemq.persistence.cluster;

import com.hivemq.persistence.cluster.address.AddressRegistry;

public interface ClusteringService {
    AddressRegistry getRegistry();
    String getNodeAddress();
    int getRPCPort();
    String getRPCServiceAddress();
}
