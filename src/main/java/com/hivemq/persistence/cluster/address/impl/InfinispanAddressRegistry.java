package com.hivemq.persistence.cluster.address.impl;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.cluster.address.Address;
import com.hivemq.persistence.cluster.address.AddressRegistry;
import com.hivemq.persistence.cluster.address.Record;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.manager.DefaultCacheManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class InfinispanAddressRegistry implements AddressRegistry {
    private static class RegistryKey {
        private final String address;
        private final String recordId;

        RegistryKey(@NotNull final Address address, @NotNull final Record record) {
            this.address = address.getValue();
            this.recordId = record.getId();
        }

        @Override
        public String toString() {
            return new Gson().toJson(this);
        }

        public String getAddress() {
            return address;
        }
    }

    private static class RegistryKeyGrouper implements Grouper<String> {
        @Override
        public Object computeGroup(final String key, final Object group) {
            try {
                final RegistryKey rKey = new Gson().fromJson(key, RegistryKey.class);

                return rKey.getAddress();
            }
            catch (JsonSyntaxException ex) {
                return null;
            }
        }

        @Override
        public Class<String> getKeyType() {
            return String.class;
        }
    }

    private final AdvancedCache<String, String> entries;

    @Inject
    public InfinispanAddressRegistry(@NotNull final DefaultCacheManager manager) {
        final Configuration config = new ConfigurationBuilder()
                .clustering()
                .cacheMode(CacheMode.DIST_ASYNC)
                .hash().groups().addGrouper(new RegistryKeyGrouper()).enabled()
                .build();

        final String cacheName = "com.hivemq.address.registry";
        manager.defineConfiguration(cacheName, config);

        final Cache<String, String> cache = manager.getCache(cacheName);

        this.entries = cache.getAdvancedCache();
    }

    @Override
    public CompletableFuture<Boolean> put(@NotNull final Address address,
                                          @NotNull final Record record,
                                          final long ttlSeconds,
                                          final boolean update) {
        final RegistryKey key = new RegistryKey(address, record);
        final String value = new Gson().toJson(record);
        final CompletableFuture<Boolean> completed;

        if (update) {
            completed = entries.putAsync(key.toString(), value, ttlSeconds, TimeUnit.SECONDS)
                    .thenApply(result -> true);
        }
        else {
            completed = entries.putIfAbsentAsync(key.toString(), value, ttlSeconds, TimeUnit.SECONDS)
                    .thenApply(Objects::isNull);
        }

        return completed;
    }

    @Override
    public CompletableFuture<Void> put(@NotNull final Set<Address> addresses,
                                       @NotNull final Record record,
                                       final long ttlSeconds,
                                       final boolean update) {
        if (addresses.size() <= 0) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.allOf(
                addresses.stream()
                .map(address -> put(address, record, ttlSeconds, update))
                .toArray(CompletableFuture[]::new)
        );
    }

    @Override
    public CompletableFuture<Collection<Record>> get(@NotNull final Address address) {
        final CompletableFuture<Collection<Record>> fetched = new CompletableFuture<>();

        final Map<String, String> groupEntries = entries.getGroup(address.getValue());

        final Gson gson = new Gson();

        final Set<Record> records = groupEntries.values()
                .stream()
                .map(value -> gson.fromJson(value, Record.class))
                .collect(Collectors.toSet());

        fetched.complete(records);

        return fetched;
    }

    @Override
    public CompletableFuture<Void> remove(@NotNull final Address address, @NotNull final Record record) {
        final RegistryKey key = new RegistryKey(address, record);

        return entries.removeAsync(key.toString())
                .thenApply(removedKey -> null);
    }
}
