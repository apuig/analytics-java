package com.segment.analytics.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.segment.analytics.internal.JSON.InstantDeserializer;
import com.segment.analytics.internal.JSON.InstantSerializer;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class Batch {
    private List<Message> batch;

    @JsonSerialize(using = InstantSerializer.class)
    @JsonDeserialize(using = InstantDeserializer.class)
    private Instant sentAt;

    private Map<String, ?> context;

    private String writeKey;

    public Batch() {
        this.sentAt = Instant.now();
    }

    public List<Message> getBatch() {
        return batch;
    }

    public void setBatch(List<Message> batch) {
        this.batch = batch;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public void setSentAt(Instant sentAt) {
        this.sentAt = sentAt;
    }

    public Map<String, ?> getContext() {
        return context;
    }

    public void setContext(Map<String, ?> context) {
        this.context = context;
    }

    public String getWriteKey() {
        return writeKey;
    }

    public void setWriteKey(String writeKey) {
        this.writeKey = writeKey;
    }
}
