package com.hivemq.persistence.cluster.address;

import com.hivemq.extension.sdk.api.annotations.NotNull;

public class Record {
    private final String id;
    private final String content;

    public Record(@NotNull final String id, @NotNull final String content) {
        this.id = id.trim();
        this.content = content.trim();
    }

    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }
}
