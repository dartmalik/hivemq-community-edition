package com.hivemq.persistence.cluster;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.cluster.address.Address;
import com.hivemq.persistence.cluster.address.AddressRegistry;
import com.hivemq.persistence.cluster.address.Record;
import com.hivemq.persistence.cluster.address.impl.InfinispanAddressRegistry;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Singleton
public class InfinispanClusteringService implements ClusteringService {
    private static final Logger log = LoggerFactory.getLogger(InfinispanClusteringService.class);
    private static final String DOMAIN_CONNECTIONS = "mqtt.connections.";
    private static final String DOMAIN_SESSIONS = "mqtt.sessions.";
    private static final String DOMAIN_SUBSCRIPTIONS = "mqtt.subscriptions.";

    private DefaultCacheManager cacheManager;
    private AddressRegistry registry;
    private int grpcPort = 3000;

    public void open() {
        grpcPort = (int)(Math.random()*(65353-1023)) + 1023;
        log.debug("opening rpc server on port: " + grpcPort);

        final GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();

        cacheManager = new DefaultCacheManager(global.build());

        registry = new InfinispanAddressRegistry(cacheManager);
    }

    @Override
    public AddressRegistry getRegistry() {
        return registry;
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

    @Override
    public CompletableFuture<InetSocketAddress> getOrRegisterSession(@NotNull final String clientId) {
        final Address address = new Address(DOMAIN_SESSIONS + clientId);
        final Record record = new Record(clientId, getRPCServiceAddress());

        return registry.getOrReplace(address, record, 48*60*60)
                .thenApply(this::parse);
    }

    @Override
    public CompletableFuture<Void> subscribeTopics(@NotNull final String clientId, @NotNull final Set<String> topics) {
        if (topics.size() <= 0) {
            return CompletableFuture.completedFuture(null);
        }

        final Record record = new Record(cacheManager.getNodeAddress(), getRPCServiceAddress());
        final Set<Address> addresses = new HashSet<>();

        for (final String topic: topics) {
            addresses.add(new Address(DOMAIN_SUBSCRIPTIONS + topic));
        }

        return registry.put(addresses, record, 48*60*60, true);
    }

    @Override
    public CompletableFuture<Set<InetSocketAddress>> getSubscribedNodes(@NotNull final String topic) {
        final Address address = new Address(DOMAIN_SUBSCRIPTIONS + topic);

        return registry.get(address)
                .thenApply(records -> records.stream().map(this::parse).collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Boolean> register(@NotNull final String clientId) {
        final Record record = new Record(clientId, getRPCServiceAddress());

        return registry.put(connection(clientId), record, 3600, false);
    }

    @Override
    public CompletableFuture<Void> unregister(@NotNull final String clientId) {
        final Record record = new Record(clientId, getRPCServiceAddress());

        return registry.remove(connection(clientId), record);
    }

    @Override
    public CompletableFuture<InetSocketAddress> getConnectionNode(@NotNull final String clientId) {
        return registry.get(connection(clientId))
                .thenApply(records -> {
                    if (Objects.isNull(records) || records.size() <= 0) {
                        return null;
                    } else {
                      return parse(records.stream().findFirst().get());
                    }
                });
    }

    private Address connection(@NotNull final String clientId) {
        return new Address(DOMAIN_CONNECTIONS + clientId);
    }

    private InetSocketAddress parse(final Record record) {
        final String[] parts = record.getContent().split(":");
        return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    }
}
