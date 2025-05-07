package com.segment.analytics.config;

public class StorageConfig extends BatchQueueConfig {
    /** path to save pending messages */
    public final String filePath;

    private StorageConfig(Builder builder) {
        super(builder);
        this.filePath = builder.filePath;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BatchQueueConfig.Builder {
        private String filePath = Defaults.DEFAULT_STORAGE_FILE;

        Builder() {
            this.size = Defaults.DEFAULT_STORAGE_QUEUE_SIZE;
            this.flushMs = Defaults.DEFAULT_STORAGE_QUEUE_FLUSH_MS;
        }

        public Builder filePath(String value) {
            this.filePath = value;
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(this);
        }
    }
}
