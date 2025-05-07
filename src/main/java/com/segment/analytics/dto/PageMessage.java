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
        setType(Type.page);
    }

    public PageMessage(final String userId, final String name, final String category) {
        super(Type.page, userId);
        setName(name);
        setCategory(category);
    }

    public PageMessage(final String userId, final String name, final Map<String, ?> properties, final String category) {
        this(userId, name, category);
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

    public String getCategory() {
        return category;
    }

    public void setCategory(final String category) {
        this.category = category;
    }
}
