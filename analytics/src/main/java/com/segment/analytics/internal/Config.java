package com.segment.analytics.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

public class Config {

    // from SegmentQueue
    public static final int DEFAULT_HTTP_QUEUE_SIZE = 250; // analytics-java Integer.MAX_VALUE;
    public static final int DEFAULT_HTTP_QUEUE_FLUSH = 50; // analytics-java 250;
    public static final int DEFAULT_HTTP_QUEUE_FLUSH_MS = 10 * 1000;

    public static final int DEFAULT_HTTP_EXECUTOR_SIZE = 1;
    public static final int DEFAULT_HTTP_EXECUTOR_QUEUE_SIZE = 0; // SegmentQueue  5;

    public static final int DEFAULT_HTTP_TIMEOUT_SECONDS = 15;

    public static final int DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE = 10;
    public static final int DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN = 30;
    public static final int DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE = 1;

    // FALLBACK
    public static final int DEFAULT_FALLBACK_QUEUE_SIZE = 250;
    public static final int DEFAULT_FALLBACK_QUEUE_FLUSH_SIZE = 50;
    public static final int DEFAULT_FALLBACK_QUEUE_FLUSH_MS = 2_000;
    public static final int DEFAULT_FALLBACK_ROLLOVER_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_FALLBACK_ROLLOVER_MAX_SIZE = 1024 * 1024 * 5;
    public static final String DEFAULT_FALLBACK_FILE = "pending";


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

        final ExecutorService executor;
        public OkHttpClient client; // Analytics touch the instance

        private HttpConfig(Builder builder) {
            this.queueSize = builder.queueSize;
            this.flushQueueSize = builder.flushQueueSize;
            this.flushIntervalInMillis = builder.flushIntervalInMillis;
            this.circuitErrorsInAMinute = builder.circuitErrorsInAMinute;
            this.circuitSecondsInOpen = builder.circuitSecondsInOpen;
            this.circuitRequestToClose = builder.circuitRequestToClose;

            this.executor = new ThreadPoolExecutor(
                    builder.executorSize,
                    builder.executorSize,
                    15,
                    TimeUnit.SECONDS,
                    builder.executorQueueSize == 0
                            ? new SynchronousQueue<>(true)
                            : new ArrayBlockingQueue<>(builder.executorQueueSize, true),
                    // this will cause the HTTP requests to be handled on AnalyticsClient.Looper
                    // SegmentQueue was discarding oldest tasks // e.getQueue().poll(); e.execute(r);
                    new CallerRunsPolicy() {
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                            // System.err.println("==== NetowrkPool exhausted, running in %s
                            // ====".formatted(Thread.currentThread().getName()));
                            super.rejectedExecution(r, e);
                        }
                    });

            this.client = new OkHttpClient.Builder()
                    .connectTimeout(builder.timeoutSeconds, TimeUnit.SECONDS)
                    .readTimeout(builder.timeoutSeconds, TimeUnit.SECONDS)
                    .writeTimeout(builder.timeoutSeconds, TimeUnit.SECONDS)
                    // use same executor
                    .dispatcher(new Dispatcher(this.executor))
                    .build();
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

            private int executorSize = DEFAULT_HTTP_EXECUTOR_SIZE;
            private int executorQueueSize = DEFAULT_HTTP_EXECUTOR_QUEUE_SIZE;
            private int timeoutSeconds = DEFAULT_HTTP_TIMEOUT_SECONDS;

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

            public Builder executorSize(int value) {
                this.executorSize = value;
                return this;
            }

            public Builder executorQueueSize(int value) {
                this.executorQueueSize = value;
                return this;
            }

            public Builder timeoutSeconds(int value) {
                this.timeoutSeconds = value;
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
        /** max time to keep a open overflow file before finish the batch */
        final int rolloverTimeoutSeconds;
        /** max size of files */
        final long rolloverMaxSizeBytes;

        private FileConfig(Builder builder) {
            this.size = builder.size;
            this.flushSize = builder.flushSize;
            this.flushMs = builder.flushMs;
            this.filePath = builder.filePath;
            this.rolloverTimeoutSeconds = builder.rolloverTimeoutSeconds;
            this.rolloverMaxSizeBytes = builder.rolloverMaxSizeBytes;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int size = DEFAULT_FALLBACK_QUEUE_SIZE;
            private int flushSize = DEFAULT_FALLBACK_QUEUE_FLUSH_SIZE;
            private int flushMs = DEFAULT_FALLBACK_QUEUE_FLUSH_MS;
            private String filePath = DEFAULT_FALLBACK_FILE;
            private int rolloverTimeoutSeconds = DEFAULT_FALLBACK_ROLLOVER_TIMEOUT_SECONDS;
            private long rolloverMaxSizeBytes = DEFAULT_FALLBACK_ROLLOVER_MAX_SIZE;

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

            public Builder rolloverTimeoutSeconds(int value) {
                this.rolloverTimeoutSeconds = value;
                return this;
            }

            public Builder rolloverMaxSizeBytes(long value) {
                this.rolloverMaxSizeBytes = value;
                return this;
            }

            public FileConfig build() {
                return new FileConfig(this);
            }
        }
    }
}
