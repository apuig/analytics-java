package com.segment.analytics.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.segment.analytics.internal.JSON.InstantDeserializer;
import com.segment.analytics.internal.JSON.InstantSerializer;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = IdentifyMessage.class, name = "identify"),
    @JsonSubTypes.Type(value = GroupMessage.class, name = "group"),
    @JsonSubTypes.Type(value = TrackMessage.class, name = "track"),
    @JsonSubTypes.Type(value = ScreenMessage.class, name = "screen"),
    @JsonSubTypes.Type(value = PageMessage.class, name = "page"),
    @JsonSubTypes.Type(value = AliasMessage.class, name = "alias"),
})
public abstract class Message {

    private Type type;

    private String messageId;

    @JsonSerialize(using = InstantSerializer.class)
    @JsonDeserialize(using = InstantDeserializer.class)
    private Instant timestamp;

    private Map<String, ?> context;

    private String anonymousId;

    private String userId;

    private Map<String, Object> integrations;

    enum Type {
        identify,
        group,
        track,
        screen,
        page,
        alias
    }

    protected Message() {}

    protected Message(final Type type, final String userId) {
        this.type = type;
        this.userId = userId;
        this.timestamp = Instant.now();
        this.messageId = UUID.randomUUID().toString();
    }

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(final String messageId) {
        this.messageId = messageId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, ?> getContext() {
        return context;
    }

    public void setContext(final Map<String, ?> context) {
        this.context = context;
    }

    public String getAnonymousId() {
        return anonymousId;
    }

    public void setAnonymousId(final String anonymousId) {
        this.anonymousId = anonymousId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(final String userId) {
        this.userId = userId;
    }

    public Map<String, Object> getIntegrations() {
        return integrations;
    }

    public void setIntegrations(final Map<String, Object> integrations) {
        this.integrations = integrations;
    }
}
