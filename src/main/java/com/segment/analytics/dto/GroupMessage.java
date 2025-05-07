package com.segment.analytics.dto;

import java.util.Map;

/**
 * The group API call is how you associate an individual user with a group—be it a company,
 * organization, account, project, team or whatever other crazy name you came up with for the same
 * concept! It also lets you record custom traits about the group, like industry or number of
 * employees. Calling group is a slightly more advanced feature, but it’s helpful if you have
 * accounts with multiple users.
 * @see <a href="https://segment.com/docs/spec/group/">Group</a>
 */
public class GroupMessage extends Message {

    private String groupId;

    private Map<String, ?> traits;

    public GroupMessage() {
        super();
        setType(Type.group);
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Map<String, ?> getTraits() {
        return traits;
    }

    public void setTraits(Map<String, ?> traits) {
        this.traits = traits;
    }
}
