package com.hivemq.cluster.address;

import com.hivemq.extension.sdk.api.annotations.NotNull;

public class Address {
    private final String value;

    public Address(@NotNull final String value) {
        this.value = value.trim();
    }

    public String getValue() {
        return value;
    }
}
