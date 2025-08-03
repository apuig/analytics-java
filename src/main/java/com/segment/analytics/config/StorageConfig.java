package com.segment.analytics.config;

public class StorageConfig extends BatchQueueConfig {
    /** path to save pending messages */
    public final String filePath;

    private StorageConfig(final Builder builder) {
        super(builder);
        this.filePath = builder.filePath;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends BatchQueueConfig.Builder<Builder> {
        private String filePath = Defaults.DEFAULT_STORAGE_FILE;

        public Builder() {
            this.size = Defaults.DEFAULT_STORAGE_QUEUE_SIZE;
            this.flushMs = Defaults.DEFAULT_STORAGE_QUEUE_FLUSH_MS;
        }

        public Builder filePath(final String value) {
            this.filePath = value;
            return this;
        }

        @Override
        public StorageConfig build() {
            return new StorageConfig(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
