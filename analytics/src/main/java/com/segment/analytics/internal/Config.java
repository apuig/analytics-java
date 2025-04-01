package com.segment.analytics.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;

public class Config {

    public static final int DEFAULT_FALLBACK_QUEUE_SIZE = 250;
    public static final int DEFAULT_FALLBACK_QUEUE_FLUSH_SIZE = 50;
    public static final int DEFAULT_FALLBACK_QUEUE_FLUSH_MS = 2_000;
    public static final String DEFAULT_FALLBACK_FILE = "pending";

    //

    public static final int DEFAULT_HTTP_QUEUE_SIZE = Integer.MAX_VALUE;
    public static final int DEFAULT_HTTP_QUEUE_FLUSH = 250;
    public static final int DEFAULT_HTTP_QUEUE_FLUSH_MS = 10 * 1000;

    public static final int DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE = 10;
    public static final int DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN = 30;
    public static final int DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE = 1;

    //

    public static OkHttpClient defaultClient() {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(15, TimeUnit.SECONDS)
                .writeTimeout(15, TimeUnit.SECONDS)
                .build();
        return client;
    }

    public static ExecutorService defaultNetworkExecutor() {
        return Executors.newSingleThreadExecutor(defaultThreadFactory());
    }

    public static ThreadFactory defaultThreadFactory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
                        r.run();
                    }
                });
            }
        };
    }

    public static class HttpConfig {
        final int queueSize;
        final int flushQueueSize;
        final long flushIntervalInMillis;

        final int circuitErrorsInAMinute;
        final int circuitSecondsInOpen;
        final int circuitRequestToClose;

        private HttpConfig(Builder builder) {
            this.queueSize = builder.queueSize;
            this.flushQueueSize = builder.flushQueueSize;
            this.flushIntervalInMillis = builder.flushIntervalInMillis;
            this.circuitErrorsInAMinute = builder.circuitErrorsInAMinute;
            this.circuitSecondsInOpen = builder.circuitSecondsInOpen;
            this.circuitRequestToClose = builder.circuitRequestToClose;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int queueSize = DEFAULT_HTTP_QUEUE_SIZE;
            private int flushQueueSize = DEFAULT_HTTP_QUEUE_FLUSH;
            private long flushIntervalInMillis = DEFAULT_HTTP_QUEUE_FLUSH_MS;

            private int circuitErrorsInAMinute = DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE;
            private int circuitSecondsInOpen = DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN;
            private int circuitRequestToClose = DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE;

            public Builder queueSize(int value) {
                if (value <= 0) {
                    throw new IllegalArgumentException("queueSize should be positive.");
                }
                this.queueSize = value;
                return this;
            }

            public Builder flushQueueSize(int value) {
                if (value < 1) {
                    throw new IllegalArgumentException("flushQueueSize must not be less than 1.");
                }
                this.flushQueueSize = value;
                return this;
            }

            public Builder flushIntervalInMillis(long value) {
                if (value < 1000) {
                    throw new IllegalArgumentException("flushIntervalInMillis must not be less than 1 second.");
                }

                this.flushIntervalInMillis = value;
                return this;
            }

            public Builder circuitErrorsInAMinute(int value) {
                this.circuitErrorsInAMinute = value;
                return this;
            }

            public Builder circuitSecondsInOpen(int value) {
                this.circuitSecondsInOpen = value;
                return this;
            }

            public Builder circuitRequestToClose(int value) {
                this.circuitRequestToClose = value;
                return this;
            }

            public HttpConfig build() {
                return new HttpConfig(this);
            }
        }
    }

    public static class FileConfig {
        /** size of the queue waiting to be written to file */
        final int size;
        /** batch size to flush messages to file*/
        final int flushSize;
        /** max milliseconds without a flush messages to file*/
        final int flushMs;
        /** path to save pending messages */
        final String filePath;

        private FileConfig(Builder builder) {
            this.size = builder.size;
            this.flushSize = builder.flushSize;
            this.flushMs = builder.flushMs;
            this.filePath = builder.filePath;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int size = DEFAULT_FALLBACK_QUEUE_SIZE;
            private int flushSize = DEFAULT_FALLBACK_QUEUE_FLUSH_SIZE;
            private int flushMs = DEFAULT_FALLBACK_QUEUE_FLUSH_MS;
            private String filePath = DEFAULT_FALLBACK_FILE;

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

            public Builder filePath(String value) {
                this.filePath = value;
                return this;
            }

            public FileConfig build() {
                return new FileConfig(this);
            }
        }
    }
}
