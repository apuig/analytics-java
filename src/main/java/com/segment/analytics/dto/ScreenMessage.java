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
        super();
        setType(Type.screen);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, ?> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, ?> properties) {
        this.properties = properties;
    }
}
