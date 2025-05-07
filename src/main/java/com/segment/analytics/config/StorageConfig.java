package com.segment.analytics.config;

/** Configuration for disk storage of pending messages. */
public class StorageConfig extends BatchQueueConfig {

    /** Path to save pending messages. */
    public final String filePath;

    private StorageConfig(final Builder builder) {
        super(builder);
        this.filePath = builder.filePath;
    }

    /** Create a new builder for StorageConfig.  */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link StorageConfig}. */
    public static final class Builder extends BatchQueueConfig.Builder<Builder> {
        private String filePath = Defaults.DEFAULT_STORAGE_FILE;

        public Builder() {
            this.size = Defaults.DEFAULT_STORAGE_QUEUE_SIZE;
            this.flushSize = Defaults.DEFAULT_STORAGE_QUEUE_FLUSH_SIZE;
            this.flushMs = Defaults.DEFAULT_STORAGE_QUEUE_FLUSH_MS;
            this.blockTimeout = Defaults.DEFAULT_STORAGE_BLOCK_TIMEOUT;
        }

        /**
         * Set the path to save pending messages.
         * @param value file path
         * @return this builder
         */
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
