package com.segment.analytics.dto;

import java.util.Map;

/**
 * The screen call lets you record whenever a user sees a screen, along with any properties aboutthe screen.
 * @see <a href="https://segment.com/docs/spec/screen/">Screen</a>
 */
public class ScreenMessage extends Message {

    private String name;

    private Map<String, ?> properties;

    public ScreenMessage() {
        setType(Type.screen);
    }

    public ScreenMessage(final String userId, final String name) {
        super(Type.screen, userId);
        setName(name);
    }

    public ScreenMessage(final String userId, final String name, final Map<String, ?> properties) {
        this(userId, name);
        setProperties(properties);
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public Map<String, ?> getProperties() {
        return properties;
    }

    public void setProperties(final Map<String, ?> properties) {
        this.properties = properties;
    }
}
