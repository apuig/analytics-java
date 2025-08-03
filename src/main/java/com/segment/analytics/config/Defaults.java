package com.segment.analytics.config;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class Defaults {

    public static final String DEFAULT_ENDPOINT = "https://api.segment.io";
    public static final String DEFAULT_PATH = "/v1/b";

    public static final int DEFAULT_QUEUE_SIZE = 1_000; // analytics-java MAX_VALUE; SegmentQueue 250
    public static final int DEFAULT_QUEUE_FLUSH_SIZE = 20; // analytics-java 250; SegmentQueue 50
    public static final int DEFAULT_QUEUE_FLUSH_MS = 30_000;

    // HTTP
    public static final int DEFAULT_HTTP_QUEUE_SIZE = DEFAULT_QUEUE_SIZE;
    public static final int DEFAULT_HTTP_QUEUE_FLUSH_SIZE = DEFAULT_QUEUE_FLUSH_SIZE;
    public static final int DEFAULT_HTTP_QUEUE_FLUSH_MS = DEFAULT_QUEUE_FLUSH_MS;

    public static final int DEFAULT_HTTP_EXECUTOR_SIZE = 2;
    public static final int DEFAULT_HTTP_EXECUTOR_QUEUE_SIZE = 0; // SegmentQueue  5;

    public static final int DEFAULT_HTTP_CONNECTION_TIMEOUT_SECONDS = 15;
    public static final int DEFAULT_HTTP_READ_TIMEOUT_SECONDS = 20;
    public static final boolean DEFAULT_HTTP_GZIP = true;

    public static final int DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE = 10;
    public static final int DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN = 30;
    public static final int DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE = 1;

    // STORAGE
    public static final int DEFAULT_STORAGE_QUEUE_SIZE = DEFAULT_QUEUE_SIZE;
    public static final int DEFAULT_STORAGE_QUEUE_FLUSH_SIZE = DEFAULT_QUEUE_FLUSH_SIZE;
    public static final int DEFAULT_STORAGE_QUEUE_FLUSH_MS = DEFAULT_QUEUE_FLUSH_MS;
    public static final String DEFAULT_STORAGE_FILE = "pending";

    // RETRY
    public static final int DEFAULT_RETRY_DELAY_SECONDS = 10; 
    public static final int DEFAULT_RETRY_INITIAL_DELAY_SECONDS = 10;
    public static final List<Duration> DEFAULT_RETRY_AT = List.of(
            Duration.ofSeconds(1),
            Duration.ofSeconds(30),
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofMinutes(15),
            Duration.ofHours(1),
            Duration.ofHours(12),
            Duration.ofDays(1),
            Duration.ofDays(4),
            Duration.ofDays(7));

    public static ThreadFactory defaultThreadFactory() {
        return new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
                        r.run();
                    }
                });
                thread.setDaemon(true);
                return thread;
            }
        };
    }
}
