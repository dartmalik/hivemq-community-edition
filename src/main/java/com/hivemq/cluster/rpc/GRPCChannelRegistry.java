package com.hivemq.cluster.rpc;

import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@LazySingleton
public class GRPCChannelRegistry {
    private final Map<String, ManagedChannel> channelMap = new ConcurrentHashMap<>();

    public ManagedChannel get(final String address, final int port) {
        return channelMap.computeIfAbsent(address+ ":" + port,
                key -> ManagedChannelBuilder.forAddress(address, port).usePlaintext().build());
    }

    public void clear() {
        for (final ManagedChannel channel : channelMap.values()) {
            channel.shutdown();
        }

        channelMap.clear();
    }
}
