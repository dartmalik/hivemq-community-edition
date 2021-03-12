package com.hivemq.persistence.cluster;

import com.hivemq.persistence.cluster.address.AddressRegistry;
import com.hivemq.persistence.cluster.address.impl.InfinispanAddressRegistry;

import org.infinispan.commons.api.AsyncCache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Singleton
public class InfinispanClusteringService implements ClusteringService {
    private static final Logger log = LoggerFactory.getLogger(InfinispanClusteringService.class);

    private DefaultCacheManager cacheManager;
    private AddressRegistry registry;
    private int grpcPort = 3000;

    public void open() {
        grpcPort = (int)(Math.random()*(65353-1023)) + 1023;

        final GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();

        cacheManager = new DefaultCacheManager(global.build());

        registry = new InfinispanAddressRegistry(cacheManager);
    }

    @Override
    public AddressRegistry getRegistry() {
        return registry;
    }

    public <K,V> AsyncCache<K,V> getCache(final String cacheName) {
        final EmbeddedCacheManagerAdmin admin = cacheManager.administration()
                                                    .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE);

        final ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.clustering().cacheMode(CacheMode.DIST_SYNC);

        return admin.getOrCreateCache(cacheName, builder.build());
    }

    @Override
    public String getNodeAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (final UnknownHostException ex) {
            log.error(ex.getMessage());
            return "";
        }
    }

    @Override
    public int getRPCPort() {
        return grpcPort;
    }

    @Override
    public String getRPCServiceAddress() {
        return getNodeAddress() + ":" + getRPCPort();
    }
}
