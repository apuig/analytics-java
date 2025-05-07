package com.segment.analytics.dto;

import java.util.Map;

/**
 * The page call lets you record whenever a user sees a page of your website, along with any properties about the page.
 * @see <a href="https://segment.com/docs/spec/page/">Page</a>
 */
public class PageMessage extends Message {

    private String name;

    private Map<String, ?> properties;

    private String category;

    public PageMessage() {
        super();
        setType(Type.page);
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
