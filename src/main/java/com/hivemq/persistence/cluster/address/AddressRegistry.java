package com.hivemq.persistence.cluster.address;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface AddressRegistry {
    String DOMAIN_SESSIONS = "mqtt.sessions";

    CompletableFuture<Boolean> put(Address address, Record record, long ttlSeconds, boolean update);
    CompletableFuture<Void> put(Set<Address> addresses, Record record, long ttlSeconds, boolean update);
    CompletableFuture<Collection<Record>> get(Address address);
    CompletableFuture<Void> remove(Address address, Record record);
}
