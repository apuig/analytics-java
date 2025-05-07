package com.segment.analytics.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Default configuration values. These values can be overridden by system properties if the builder does not specify a value.
 */
public final class Defaults {
    private static final Logger LOGGER = Logger.getLogger(Defaults.class.getName());

    private Defaults() {}

    // queue
    /** Default queue size for all queues. */
    static final int DEFAULT_QUEUE_SIZE = Integer.getInteger("segment.queue.size", 1_000);
    /** Default flush size for all queues. */
    static final int DEFAULT_QUEUE_FLUSH_SIZE = Integer.getInteger("segment.queue.flushSize", 50);
    /** Default flush interval (ms) for all queues. */
    static final int DEFAULT_QUEUE_FLUSH_MS = Integer.getInteger("segment.queue.flushMs", 30_000);
    /** Default block timeout (ms) for all queues. */
    static final int DEFAULT_BLOCK_TIMEOUT = Integer.getInteger("segment.queue.blockTimeout", 0);

    // HTTP queue
    /** Size of the HTTP upload queue. */
    static final int DEFAULT_HTTP_QUEUE_SIZE = Integer.getInteger("segment.queue.http.size", DEFAULT_QUEUE_SIZE);
    /** Max number of elements in an HTTP batch. */
    static final int DEFAULT_HTTP_QUEUE_FLUSH_SIZE =
            Integer.getInteger("segment.queue.http.flushSize", DEFAULT_QUEUE_FLUSH_SIZE);
    /** Max milliseconds without an HTTP batch flush. */
    static final int DEFAULT_HTTP_QUEUE_FLUSH_MS =
            Integer.getInteger("segment.queue.http.flushMs", DEFAULT_QUEUE_FLUSH_MS);
    /** Max milliseconds to wait to put an element in the HTTP queue. */
    static final int DEFAULT_HTTP_BLOCK_TIMEOUT =
            Integer.getInteger("segment.queue.http.blockTimeout", DEFAULT_BLOCK_TIMEOUT);

    // HTTP upload
    /** Number of failures in 1 minute to open the HTTP circuit breaker. */
    static final int DEFAULT_HTTP_CIRCUIT_ERRORS_IN_A_MINUTE =
            Integer.getInteger("segment.http.circuitErrorsInAMinute", 10);
    /** Seconds to wait in open state before half-open for HTTP circuit breaker. */
    static final int DEFAULT_HTTP_CIRCUIT_SECONDS_IN_OPEN = Integer.getInteger("segment.http.circuitSecondsInOpen", 30);
    /** Number of successes to close the HTTP circuit breaker. */
    static final int DEFAULT_HTTP_CIRCUIT_REQUESTS_TO_CLOSE =
            Integer.getInteger("segment.http.circuitRequestsToClose", 1);
    /** Max number of concurrent HTTP upload requests. */
    static final int DEFAULT_HTTP_EXECUTOR_SIZE = Integer.getInteger("segment.http.executorSize", 2);
    /** Max number of HTTP upload requests waiting to be executed. */
    static final int DEFAULT_HTTP_EXECUTOR_QUEUE_SIZE = Integer.getInteger("segment.http.executorQueueSize", 0);
    /** Max seconds to wait to establish an HTTP connection. */
    static final int DEFAULT_HTTP_CONNECTION_TIMEOUT_SECONDS =
            Integer.getInteger("segment.http.connectionTimeoutSeconds", 15);
    /** Max seconds to wait for an HTTP reply. */
    static final int DEFAULT_HTTP_READ_TIMEOUT_SECONDS = Integer.getInteger("segment.http.readTimeoutSeconds", 20);
    /** Use compression on the HTTP request. */
    static final boolean DEFAULT_HTTP_GZIP = Boolean.parseBoolean(System.getProperty("segment.http.gzip", "true"));

    // Storage queue
    /** Size of the disk storage queue. */
    static final int DEFAULT_STORAGE_QUEUE_SIZE = Integer.getInteger("segment.queue.storage.size", DEFAULT_QUEUE_SIZE);
    /** Max number of elements in a disk batch. */
    static final int DEFAULT_STORAGE_QUEUE_FLUSH_SIZE =
            Integer.getInteger("segment.queue.storage.flushSize", DEFAULT_QUEUE_FLUSH_SIZE);
    /** Max milliseconds without a disk batch flush. */
    static final int DEFAULT_STORAGE_QUEUE_FLUSH_MS =
            Integer.getInteger("segment.queue.storage.flushMs", DEFAULT_QUEUE_FLUSH_MS);
    /** Max milliseconds to wait to put an element in the disk queue. */
    static final int DEFAULT_STORAGE_BLOCK_TIMEOUT =
            Integer.getInteger("segment.queue.storage.blockTimeout", DEFAULT_BLOCK_TIMEOUT);
    /** Path to save pending messages. */
    static final String DEFAULT_STORAGE_FILE = System.getProperty("segment.storage.file", "segment-pending-batches");

    // Retry
    /** Seconds to wait between retry executions. */
    static final int DEFAULT_RETRY_DELAY_SECONDS = Integer.getInteger("segment.retry.delaySeconds", 60);
    /** Seconds to wait before the first retry execution. */
    static final int DEFAULT_RETRY_INITIAL_DELAY_SECONDS = Integer.getInteger("segment.retry.initialDelaySeconds", 60);
    /** Sequence of retry delays. */
    static final List<Duration> DEFAULT_RETRY_AT = getDurationList(
            "segment.retry.at",
            List.of(
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(30),
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(5),
                    Duration.ofMinutes(15),
                    Duration.ofHours(1),
                    Duration.ofHours(12),
                    Duration.ofDays(1),
                    Duration.ofDays(4),
                    Duration.ofDays(7),
                    Duration.ofDays(30),
                    Duration.ofDays(60)));

    // Endpoint
    /** Default Segment API endpoint. */
    public static final String DEFAULT_ENDPOINT = System.getProperty("segment.endpoint", "https://api.segment.io");
    /** Default Segment API path. */
    public static final String DEFAULT_PATH = System.getProperty("segment.endpoint.path", "/v1/b");

    /**
     * Parses a comma-separated list of durations from a system property.
     * Supported suffixes:
     *   s = seconds (default if no suffix)
     *   m = minutes
     *   h = hours
     *   d = days
     * Example: "10s,2m,1h,1d,30" (30 is 30 seconds)
     */
    static List<Duration> getDurationList(final String key, final List<Duration> def) {
        final String val = System.getProperty(key);
        if ((val == null) || val.trim().isEmpty()) {
            return def;
        }
        try {
            final List<Duration> durations = Arrays.stream(val.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(Defaults::parseDuration)
                    .collect(Collectors.toList());
            Collections.sort(durations);
            return durations;
        } catch (final Exception e) {
            LOGGER.log(Level.WARNING, e, () -> "Invalid Duration list property '%s' for '%s'".formatted(val, key));
            return def;
        }
    }

    /**
     * Parses a single duration string with optional unit suffix.
     * Supported: s (seconds), m (minutes), h (hours), d (days).
     * No suffix = seconds.
     */
    private static Duration parseDuration(final String s) {
        if ((s == null) || s.isEmpty()) {
            throw new IllegalArgumentException("Empty duration");
        }
        final String trimmed = s.trim();
        final char last = trimmed.charAt(trimmed.length() - 1);
        long value;
        if (!Character.isLetter(last)) {
            value = Long.parseLong(trimmed);
            return Duration.ofSeconds(value);
        }
        final String numberPart = trimmed.substring(0, trimmed.length() - 1).trim();
        value = Long.parseLong(numberPart);
        return switch (last) {
            case 's' -> Duration.ofSeconds(value);
            case 'm' -> Duration.ofMinutes(value);
            case 'h' -> Duration.ofHours(value);
            case 'd' -> Duration.ofDays(value);
            default -> throw new IllegalArgumentException("Unknown duration suffix: " + last);
        };
    }

    /** Daemon thread factory */
    public static java.util.concurrent.ThreadFactory defaultThreadFactory() {
        return r -> {
            final Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        };
    }
}
