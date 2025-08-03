package com.segment.analytics.dto;

import java.util.Map;
import java.util.UUID;

/**
 * The track API call is how you record any actions your users perform, along with any properties that describe the action.
 * @see <a href="https://segment.com/docs/spec/track">Track</a>
 */
public class TrackMessage extends Message {

    private String event;

    private Map<String, ?> properties;

    public TrackMessage() {
        super();
        setType(Type.track);
    }

    public TrackMessage(String userId, String event) {
        this();
        setUserId(userId);
        setEvent(event);
        setMessageId(UUID.randomUUID().toString());
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Map<String, ?> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, ?> properties) {
        this.properties = properties;
    }
}
