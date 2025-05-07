package com.segment.analytics.config;

public class Constants {
    private Constants() {}

    public static final String VERSION = "custom";

    public static final int MSG_MAX_SIZE = 1024 * 32;
    /**
     * comment from analytics-kotlin:
     * > Our servers only accept batches < 500KB.
     * > This limit is 475KB to account for extra data that is not present in payloads themselves,
     * > but is added later, such as `sentAt`, `integrations` and other json tokens.
     *
     * We probably can try to use all the 500KB, but keeping it to be safe
     */
    public static final int MAX_BATCH_SIZE = 475 * 1024;
}
