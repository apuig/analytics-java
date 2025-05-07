package com.segment.analytics.internal;

import com.segment.analytics.config.Constants;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.dto.Batch;
import dev.failsafe.CircuitBreaker;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Perform HTTP upload of {@link Batch}, keeps a circuit-breaker in order to fail fast.
 * */
public class Upload implements BatchConsumer<Batch>, Closeable {
    private static final Logger LOGGER = Logger.getLogger(Upload.class.getName());

    private final HttpClient client;
    private final HttpRequest.Builder requestBuilder;
    private final CircuitBreaker<?> breaker;
    private final ExecutorService networkExecutor;
    private final BatchConsumer<Batch> retryUpload;
    private final boolean useGzip;
    private final int readTimeoutSeconds;

    public Upload(final HttpConfig config, final URI uri, final BatchConsumer<Batch> retryUpload) {
        this.retryUpload = retryUpload;
        this.breaker = CircuitBreaker.builder()
                .withFailureThreshold(config.circuitErrorsInAMinute, Duration.ofMinutes(1))
                .withDelay(Duration.ofSeconds(config.circuitSecondsInOpen))
                .withSuccessThreshold(config.circuitRequestToClose)
                .onOpen(el -> LOGGER.log(Level.INFO, "OPEN: failing requests"))
                .onHalfOpen(el -> LOGGER.log(Level.INFO, "HALF OPEN: checking status"))
                .onClose(el -> LOGGER.log(Level.INFO, "CLOSED: attending requests normally"))
                .build();

        this.networkExecutor = new ThreadPoolExecutor(
                config.executorSize,
                config.executorSize,
                15,
                TimeUnit.SECONDS,
                config.executorQueueSize == 0
                        ? new SynchronousQueue<>(true)
                        : new ArrayBlockingQueue<>(config.executorQueueSize, true),
                new ThreadFactory() {
                    private final AtomicInteger threadCount = new AtomicInteger(1);

                    @Override
                    public Thread newThread(final Runnable r) {
                        final Thread thread = new Thread(r);
                        thread.setName("segment-http-" + threadCount.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                // NOTE: it can blocks on:
                // RetryUpload scheduled task
                // BatchQueue thread consuming the queue
                new CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(final Runnable r, final ThreadPoolExecutor e) {
                        LOGGER.log(
                                Level.FINE,
                                () -> "networkPool exhausted, running in "
                                        + Thread.currentThread().getName());
                        super.rejectedExecution(r, e);
                    }
                });

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(config.connectionTimeoutSeconds))
                .followRedirects(Redirect.NORMAL)
                .build();

        this.requestBuilder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(config.readTimeoutSeconds))
                .header("User-Agent", "analytics-java/" + Constants.VERSION)
                .header("Content-Type", "text/plain");
        if (config.gzip) {
            requestBuilder.header("Content-Encoding", "gzip");
        }
        this.useGzip = config.gzip;
        this.readTimeoutSeconds = config.readTimeoutSeconds;
    }

    public void upload(final Batch batch) {
        final byte[] payload = JSON.toJson(batch);
        final BodyPublisher publisher = BodyPublishers.ofByteArray(useGzip ? gzip(payload) : payload);

        @SuppressWarnings("unused")
        final Future<?> unused = networkExecutor.submit(new UploadTask(
                breaker,
                client,
                requestBuilder,
                publisher,
                () -> {}, // no-retry
                () -> retryUpload.safeProcess(batch)));
    }

    private static byte[] gzip(final byte[] input) {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
                gzip.write(input);
            }
            return bos.toByteArray();
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to gzip payload", e);
        }
    }

    public void retry(final Path tmpFile, final BatchConsumer<Path> noRetry, final BatchConsumer<Path> retry) {
        try {
            @SuppressWarnings("unused")
            final Future<?> unused = networkExecutor.submit(new UploadTask(
                    breaker,
                    client,
                    requestBuilder,
                    useGzip
                            ? BodyPublishers.ofByteArray(gzip(Files.readAllBytes(tmpFile)))
                            : BodyPublishers.ofFile(tmpFile),
                    () -> noRetry.safeProcess(tmpFile),
                    () -> retry.safeProcess(tmpFile)));
        } catch (final FileNotFoundException e) {
            // Concurrent removed, all ok
            LOGGER.log(Level.FINE, () -> "already removed " + tmpFile);
        } catch (final IOException e) {
            LOGGER.log(Level.WARNING, e, () -> "Failed to read for gzip from file " + tmpFile);
            retry.safeProcess(tmpFile);
        }
    }

    static class UploadTask implements Runnable {
        final CircuitBreaker<?> breaker;
        final HttpClient client;
        final HttpRequest.Builder requestBuilder;
        final BodyPublisher body;
        final Runnable noRetry;
        final Runnable retry;

        public UploadTask(
                final CircuitBreaker<?> breaker,
                final HttpClient client,
                final HttpRequest.Builder requestBuilder,
                final BodyPublisher body,
                final Runnable noRetry,
                final Runnable retry) {
            this.breaker = breaker;
            this.client = client;
            this.requestBuilder = requestBuilder.copy();
            this.body = body;
            this.noRetry = noRetry;
            this.retry = retry;
        }

        @Override
        public void run() {
            boolean needsRetry = true;
            if (breaker.tryAcquirePermit()) {
                try {
                    needsRetry = post();
                } catch (final InterruptedException e) {
                    LOGGER.log(Level.WARNING, "interrupted", e);
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    LOGGER.log(Level.WARNING, "upload", e);
                    breaker.recordException(e);
                }
            }
            if (needsRetry) {
                retry.run();
            } else {
                noRetry.run();
            }
        }

        /**
         * @return needs retry
         * */
        private boolean post() throws IOException, InterruptedException {
            final HttpRequest request = requestBuilder.POST(body).build();
            final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

            final int statusCode = response.statusCode();
            if (statusCode < 300) {
                LOGGER.log(Level.FINE, "upload succeed");
                breaker.recordSuccess();
                return false;
            }
            LOGGER.log(Level.WARNING, () -> "upload %d - %s".formatted(statusCode, response.body()));
            if (statusCode == 429) {
                LOGGER.log(Level.SEVERE, "rate limit reached");
                breaker.open();
            } else if ((statusCode >= 400) && (statusCode < 500)) {
                LOGGER.log(Level.SEVERE, "BadRequest, do not retry");
                // do not retry
                // do not update circuit state
                return false;
            } else {
                breaker.recordFailure();
            }
            return true;
        }
    }

    @Override
    public void close() {
        try {
            networkExecutor.shutdownNow();
            if (!networkExecutor.awaitTermination(readTimeoutSeconds, TimeUnit.SECONDS)) {
                LOGGER.log(Level.SEVERE, "Pending HTTP requests not canceled in time");
            }
        } catch (final InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Interrupted while stopping networkExecutor");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void process(final Batch batch) throws Exception {
        upload(batch);
    }
}
