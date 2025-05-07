package com.segment.analytics.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RetryConfig {
    /** seconds to wait between executions */
    public final int delaySeconds;
    /** seconds to wait before the first executions */
    public final int initialDelaySeconds;
    /** sequence of retry delays */
    public final List<Duration> retryAt;

    private RetryConfig(Builder builder) {
        this.delaySeconds = builder.delaySeconds;
        this.initialDelaySeconds = builder.initialDelaySeconds;
        this.retryAt = builder.retryAt;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int delaySeconds = Defaults.DEFAULT_RETRY_DELAY_SECONDS;
        private int initialDelaySeconds = Defaults.DEFAULT_RETRY_INITIAL_DELAY_SECONDS;
        private List<Duration> retryAt = Defaults.DEFAULT_RETRY_AT;

        public Builder delaySeconds(int delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        public Builder initialDelaySeconds(int initialDelaySeconds) {
            this.initialDelaySeconds = initialDelaySeconds;
            return this;
        }

        public Builder retryAt(List<Duration> retryAt) {
            this.retryAt = new ArrayList<>(retryAt);
            Collections.sort(this.retryAt);
            return this;
        }

        public RetryConfig build() {
            if (retryAt == null || retryAt.isEmpty()) {
                throw new IllegalArgumentException("Invalid retryAt");
            }
            return new RetryConfig(this);
        }
    }
}
