package com.segment.analytics.dto;

import java.util.Map;

/**
 * The identify call ties a customer and their actions to a recognizable ID and traits like their email, name, etc.
 * @see <a href="https://segment.com/docs/spec/identify/">Identify</a>
 */
public class IdentifyMessage extends Message {

    private Map<String, ?> traits;

    public IdentifyMessage() {
        super();
        setType(Type.identify);
    }

    public Map<String, ?> getTraits() {
        return traits;
    }

    public void setTraits(Map<String, ?> traits) {
        this.traits = traits;
    }
}
