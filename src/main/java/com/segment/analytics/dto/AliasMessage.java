package com.segment.analytics.dto;

/**
 * The alias message is used to merge two user identities, effectively connecting two sets of user data as one.
 * This is an advanced method, but it is required to manage user identities successfully in some of our integrations.
 * @see <a href="https://segment.com/docs/spec/alias/">Alias</a>
 */
public class AliasMessage extends Message {

    private String previousId;

    public AliasMessage() {
        setType(Type.alias);
    }

    public AliasMessage(final String userId, final String previousId) {
        super(Type.alias, userId);
        setPreviousId(previousId);
    }

    public String getPreviousId() {
        return previousId;
    }

    public void setPreviousId(final String previousId) {
        this.previousId = previousId;
    }
}
