package com.segment.analytics.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for retrying failed uploads from disk.
 * <p>
 * Failed uploads are retried according to the configured schedule. Once the last retry delay is exhausted,
 * the pending batch is deleted and will not be retried again.
 */
public final class RetryConfig {
    /** Seconds to wait between executions. */
    public final int delaySeconds;
    /** Seconds to wait before the first execution. */
    public final int initialDelaySeconds;
    /** Sequence of retry delays. */
    public final List<Duration> retryAt;

    private RetryConfig(final Builder builder) {
        this.delaySeconds = builder.delaySeconds;
        this.initialDelaySeconds = builder.initialDelaySeconds;
        this.retryAt = Collections.unmodifiableList(builder.retryAt);
    }

    /** Create a new builder for RetryConfig. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link RetryConfig}. */
    public static final class Builder {
        private int delaySeconds = Defaults.DEFAULT_RETRY_DELAY_SECONDS;
        private int initialDelaySeconds = Defaults.DEFAULT_RETRY_INITIAL_DELAY_SECONDS;
        private List<Duration> retryAt = new ArrayList<>(Defaults.DEFAULT_RETRY_AT);

        /** Set the seconds to wait between executions. */
        public Builder delaySeconds(final int delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        /** Set the seconds to wait before the first execution. */
        public Builder initialDelaySeconds(final int initialDelaySeconds) {
            this.initialDelaySeconds = initialDelaySeconds;
            return this;
        }

        /** Set the sequence of retry delays. */
        public Builder retryAt(final List<Duration> retryAt) {
            this.retryAt = new ArrayList<>(retryAt);
            Collections.sort(this.retryAt);
            return this;
        }

        /** Build the {@link RetryConfig} instance. */
        public RetryConfig build() {
            if ((retryAt == null) || retryAt.isEmpty()) {
                throw new IllegalArgumentException("Invalid retryAt");
            }
            return new RetryConfig(this);
        }
    }
}
