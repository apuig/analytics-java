package com.segment.analytics.config;

public class HttpConfig extends BatchQueueConfig {
    /** X failure in 1 minute open the circuit*/
    public final int circuitErrorsInAMinute;
    /** once open wait X seconds to be half-open*/
    public final int circuitSecondsInOpen;
    /** after X success the circuit is closed*/
    public final int circuitRequestToClose;
    /** max seconds to wait to establish a connection */
    public final int connectionTimeoutSeconds;
    /** max seconds to wait for a reply */
    public final int readTimeoutSeconds;
    /** use compression on the request */
    public final boolean gzip;
    /** max number of concurrent HTTP upload requests */
    public final int executorSize;
    /** max number of HTTP upload requests waiting to be executed */
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends BatchQueueConfig.Builder<Builder> {
        private int circuitErrorsInAMinute = Defaults.DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE;
        private int circuitSecondsInOpen = Defaults.DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN;
        private int circuitRequestToClose = Defaults.DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE;

        private int executorSize = Defaults.DEFAULT_HTTP_EXECUTOR_SIZE;
        private int executorQueueSize = Defaults.DEFAULT_HTTP_EXECUTOR_QUEUE_SIZE;
        private int connectionTimeoutSeconds = Defaults.DEFAULT_HTTP_CONNECTION_TIMEOUT_SECONDS;
        private int readTimeoutSeconds = Defaults.DEFAULT_HTTP_READ_TIMEOUT_SECONDS;

        private boolean gzip;

        public Builder() {
            this.size = Defaults.DEFAULT_HTTP_QUEUE_SIZE;
            this.flushSize = Defaults.DEFAULT_HTTP_QUEUE_FLUSH_SIZE;
            this.flushMs = Defaults.DEFAULT_HTTP_QUEUE_FLUSH_MS;
        }

        public Builder circuitErrorsInAMinute(final int value) {
            this.circuitErrorsInAMinute = value;
            return this;
        }

        public Builder circuitSecondsInOpen(final int value) {
            this.circuitSecondsInOpen = value;
            return this;
        }

        public Builder circuitRequestToClose(final int value) {
            this.circuitRequestToClose = value;
            return this;
        }

        public Builder executorSize(final int value) {
            this.executorSize = value;
            return this;
        }

        public Builder executorQueueSize(final int value) {
            this.executorQueueSize = value;
            return this;
        }

        public Builder connectionTimeoutSeconds(final int value) {
            this.connectionTimeoutSeconds = value;
            return this;
        }

        public Builder readTimeoutSeconds(final int value) {
            this.readTimeoutSeconds = value;
            return this;
        }

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
