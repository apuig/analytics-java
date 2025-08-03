package com.segment.analytics.config;

public class BatchQueueConfig {
    /** size of the queue waiting to create a batch */
    public final int size;
    /** max number of elements in a batch */
    public final int flushSize;
    /** max milliseconds without a batch flush */
    public final int flushMs;

    protected BatchQueueConfig(final Builder<?> builder) {
        this.size = builder.size;
        this.flushSize = builder.flushSize;
        this.flushMs = builder.flushMs;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public static class Builder<T extends Builder<T>> {
        protected int size;
        protected int flushSize;
        protected int flushMs;

        public T size(final int value) {
            this.size = value;
            return self();
        }

        public T flushSize(final int value) {
            this.flushSize = value;
            return self();
        }

        public T flushMs(final int value) {
            this.flushMs = value;
            return self();
        }

        public BatchQueueConfig build() {
            return new BatchQueueConfig(this);
        }

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }
    }
}
