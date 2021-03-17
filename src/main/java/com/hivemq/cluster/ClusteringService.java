package com.hivemq.cluster;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.cluster.address.AddressRegistry;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface ClusteringService {
    AddressRegistry getRegistry();
    String getNodeAddress();
    int getRPCPort();
    String getRPCServiceAddress();

    CompletableFuture<InetSocketAddress> getOrRegisterSession(@NotNull final String clientId);

    CompletableFuture<Void> subscribeTopics(@NotNull final String clientId, @NotNull final Set<String> topics);
    CompletableFuture<Set<InetSocketAddress>> getSubscribedNodes(@NotNull final String topic);

    CompletableFuture<Boolean> register(@NotNull final String clientId);
    CompletableFuture<Void> unregister(@NotNull final String clientId);
    CompletableFuture<InetSocketAddress> getConnectionNode(@NotNull String clientId);
}
