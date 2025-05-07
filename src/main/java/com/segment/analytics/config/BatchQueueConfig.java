package com.segment.analytics.config;

/**
 * Configuration for a batch queue, controlling batching and queueing behavior.
 */
public class BatchQueueConfig {

    /** Size of the queue waiting to create a batch. */
    public final int size;

    /** Maximum number of elements in a batch. */
    public final int flushSize;

    /** Maximum milliseconds without a batch flush. */
    public final int flushMs;

    /** Maximum milliseconds to wait to put an element in the queue.  */
    public final int blockTimeout;

    protected BatchQueueConfig(final Builder<?> builder) {
        this.size = builder.size;
        this.flushSize = builder.flushSize;
        this.flushMs = builder.flushMs;
        this.blockTimeout = builder.blockTimeout;
    }

    /** Create a new builder for BatchQueueConfig. */
    public static Builder<?> builder() {
        return new Builder<>();
    }

    /** Builder for {@link BatchQueueConfig}. */
    public static class Builder<T extends Builder<T>> {
        protected int size;
        protected int flushSize;
        protected int flushMs;
        protected int blockTimeout;

        /**
         * Set the size of the queue waiting to create a batch.
         *
         * @param value queue size
         * @return this builder
         */
        public T size(final int value) {
            this.size = value;
            return self();
        }

        /**
         * Set the maximum number of elements in a batch.
         *
         * @param value batch size
         * @return this builder
         */
        public T flushSize(final int value) {
            this.flushSize = value;
            return self();
        }

        /**
         * Set the maximum milliseconds without a batch flush.
         *
         * @param value flush interval in ms
         * @return this builder
         */
        public T flushMs(final int value) {
            this.flushMs = value;
            return self();
        }

        /**
         * Set the maximum milliseconds to wait to put an element in the queue.
         *
         * @param value timeout in ms
         * @return this builder
         */
        public T blockTimeout(final int value) {
            this.blockTimeout = value;
            return self();
        }

        /**
         * Build the {@link BatchQueueConfig} instance.
         */
        public BatchQueueConfig build() {
            return new BatchQueueConfig(this);
        }

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }
    }
}
