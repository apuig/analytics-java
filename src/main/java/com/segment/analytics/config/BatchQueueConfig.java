package com.segment.analytics.config;

public class BatchQueueConfig {
    /** size of the queue waiting to create a batch */
    public final int size;
    /** max number of elements in a batch */
    public final int flushSize;
    /** max milliseconds without a batch flush */
    public final int flushMs;

    protected BatchQueueConfig(Builder builder) {
        this.size = builder.size;
        this.flushSize = builder.flushSize;
        this.flushMs = builder.flushMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        protected int size;
        protected int flushSize;
        protected int flushMs;

        public Builder size(int value) {
            this.size = value;
            return this;
        }

        public Builder flushSize(int value) {
            this.flushSize = value;
            return this;
        }

        public Builder flushMs(int value) {
            this.flushMs = value;
            return this;
        }

        public BatchQueueConfig build() {
            return new BatchQueueConfig(this);
        }
    }
}
