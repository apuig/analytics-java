package com.segment.analytics.dto;

import java.util.Map;

/**
 * The track API call is how you record any actions your users perform, along with any properties that describe the action.
 * @see <a href="https://segment.com/docs/spec/track">Track</a>
 */
public class TrackMessage extends Message {

    private String event;

    private Map<String, ?> properties;

    public TrackMessage() {
        setType(Type.track);
    }

    public TrackMessage(final String userId, final String event) {
        super(Type.track, userId);
        setEvent(event);
    }

    public TrackMessage(final String userId, final String event, final Map<String, ?> properties) {
        this(userId, event);
        setProperties(properties);
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(final String event) {
        this.event = event;
    }

    public Map<String, ?> getProperties() {
        return properties;
    }

    public void setProperties(final Map<String, ?> properties) {
        this.properties = properties;
    }
}
