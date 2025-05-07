package com.segment.analytics.config;

/** Configuration for HTTP upload, including batching, circuit breaker, and connection settings. */
public class HttpConfig extends BatchQueueConfig {

    /** Number of failures in 1 minute to open the circuit. */
    public final int circuitErrorsInAMinute;

    /** Once open, wait this many seconds to be half-open. */
    public final int circuitSecondsInOpen;

    /** After this many successes, the circuit is closed. */
    public final int circuitRequestToClose;

    /** Maximum seconds to wait to establish a connection. */
    public final int connectionTimeoutSeconds;

    /** Maximum seconds to wait for a reply. */
    public final int readTimeoutSeconds;

    /** Use compression on the request. */
    public final boolean gzip;

    /** Maximum number of concurrent HTTP upload requests. */
    public final int executorSize;

    /** Maximum number of HTTP upload requests waiting to be executed. */
    public final int executorQueueSize;

    private HttpConfig(final Builder builder) {
        super(builder);
        this.circuitErrorsInAMinute = builder.circuitErrorsInAMinute;
        this.circuitSecondsInOpen = builder.circuitSecondsInOpen;
        this.circuitRequestToClose = builder.circuitRequestToClose;
        this.connectionTimeoutSeconds = builder.connectionTimeoutSeconds;
        this.readTimeoutSeconds = builder.readTimeoutSeconds;
        this.gzip = builder.gzip;
        this.executorSize = builder.executorSize;
        this.executorQueueSize = builder.executorQueueSize;
    }

    /** Create a new builder for HttpConfig. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link HttpConfig}. */
    public static final class Builder extends BatchQueueConfig.Builder<Builder> {
        private int circuitErrorsInAMinute = Defaults.DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE;
        private int circuitSecondsInOpen = Defaults.DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN;
        private int circuitRequestToClose = Defaults.DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE;

        private int executorSize = Defaults.DEFAULT_HTTP_EXECUTOR_SIZE;
        private int executorQueueSize = Defaults.DEFAULT_HTTP_EXECUTOR_QUEUE_SIZE;
        private int connectionTimeoutSeconds = Defaults.DEFAULT_HTTP_CONNECTION_TIMEOUT_SECONDS;
        private int readTimeoutSeconds = Defaults.DEFAULT_HTTP_READ_TIMEOUT_SECONDS;

        private boolean gzip = Defaults.DEFAULT_HTTP_GZIP;

        public Builder() {
            this.size = Defaults.DEFAULT_HTTP_QUEUE_SIZE;
            this.flushSize = Defaults.DEFAULT_HTTP_QUEUE_FLUSH_SIZE;
            this.flushMs = Defaults.DEFAULT_HTTP_QUEUE_FLUSH_MS;
            this.blockTimeout = Defaults.DEFAULT_HTTP_BLOCK_TIMEOUT;
        }

        /**
         * Set the number of failures in 1 minute to open the circuit.
         * @param value failure count
         * @return this builder
         */
        public Builder circuitErrorsInAMinute(final int value) {
            this.circuitErrorsInAMinute = value;
            return this;
        }

        /**
         * Set the seconds to wait in open state before half-open.
         * @param value seconds
         * @return this builder
         */
        public Builder circuitSecondsInOpen(final int value) {
            this.circuitSecondsInOpen = value;
            return this;
        }

        /**
         * Set the number of successes to close the circuit.
         * @param value success count
         * @return this builder
         */
        public Builder circuitRequestToClose(final int value) {
            this.circuitRequestToClose = value;
            return this;
        }

        /**
         * Set the maximum number of concurrent HTTP upload requests.
         * @param value thread pool size
         * @return this builder
         */
        public Builder executorSize(final int value) {
            this.executorSize = value;
            return this;
        }

        /**
         * Set the maximum number of HTTP upload requests waiting to be executed.
         * @param value queue size
         * @return this builder
         */
        public Builder executorQueueSize(final int value) {
            this.executorQueueSize = value;
            return this;
        }

        /**
         * Set the maximum seconds to wait to establish a connection.
         * @param value seconds
         * @return this builder
         */
        public Builder connectionTimeoutSeconds(final int value) {
            this.connectionTimeoutSeconds = value;
            return this;
        }

        /**
         * Set the maximum seconds to wait for a reply.
         * @param value seconds
         * @return this builder
         */
        public Builder readTimeoutSeconds(final int value) {
            this.readTimeoutSeconds = value;
            return this;
        }

        /**
         * Enable or disable compression on the request.
         * @param value true to enable gzip
         * @return this builder
         */
        public Builder gzip(final boolean value) {
            this.gzip = value;
            return this;
        }

        @Override
        public HttpConfig build() {
            return new HttpConfig(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
