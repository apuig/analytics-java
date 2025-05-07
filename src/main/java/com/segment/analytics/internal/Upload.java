package com.segment.analytics.internal;

import com.segment.analytics.config.Constants;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.dto.Batch;
import dev.failsafe.CircuitBreaker;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * Perform HTTP upload of {@link Batch}, keeps a circuit-breaker in order to fail fast.
 * */
public class Upload implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(Upload.class.getName());

    private final HttpClient client;
    private final HttpRequest.Builder requestBuilder;
    private final CircuitBreaker<?> breaker;
    private final ExecutorService networkExecutor;
    private final Consumer<Batch> retryUpload;

    public Upload(HttpConfig config, URI uri, Consumer<Batch> retryUpload) {
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
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("segment-http-" + threadCount.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                new CallerRunsPolicy() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        LOGGER.log(
                                Level.FINEST,
                                "networkPool exhausted, running in {0}",
                                Thread.currentThread().getName());
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
    }

    public void upload(Batch batch) {
        networkExecutor.submit(new UploadTask(
                breaker,
                client,
                requestBuilder,
                BodyPublishers.ofByteArray(JSON.toJson(batch)),
                () -> {}, // no-retry
                () -> retryUpload.accept(batch)));
    }

    public void retry(Path tmpFile, Consumer<Path> noRetry, Consumer<Path> retry) {
        try {
            networkExecutor.submit(new UploadTask(
                    breaker,
                    client,
                    requestBuilder,
                    BodyPublishers.ofFile(tmpFile),
                    () -> noRetry.accept(tmpFile),
                    () -> retry.accept(tmpFile)));
        } catch (FileNotFoundException e) {
            // Concurrent removed, all ok
            LOGGER.log(Level.FINE, "already removed {0}", tmpFile);
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
                CircuitBreaker<?> breaker,
                HttpClient client,
                HttpRequest.Builder requestBuilder,
                BodyPublisher body,
                Runnable noRetry,
                Runnable retry) {
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
                    HttpRequest request = requestBuilder.POST(body).build();
                    HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

                    int statusCode = response.statusCode();
                    if (statusCode >= 300) {
                        LOGGER.log(Level.WARNING, "upload {0} - {1}", new Object[] {statusCode, response.body()});
                        if (statusCode == 429) {
                            LOGGER.log(Level.SEVERE, "rate limit reached");
                            breaker.open();
                        } else if (statusCode >= 400 && statusCode < 500) {
                            LOGGER.log(Level.SEVERE, "BadRequest, do not retry");
                            // do not retry
                            // do not update circuit state
                            needsRetry = false;
                        } else {
                            breaker.recordFailure();
                        }
                    } else {
                        LOGGER.log(Level.FINE, "upload succeed");
                        breaker.recordSuccess();
                        needsRetry = false;
                    }

                } catch (Exception e) {
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
    }

    @Override
    public void close() {
        try {
            networkExecutor.shutdownNow();
            if (!networkExecutor.awaitTermination(Defaults.DEFAULT_HTTP_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.log(Level.SEVERE, "Pending HTTP requests not canceled in time");
            }
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Interrupted while stopping networkExecutor");
            Thread.currentThread().interrupt();
        }
    }
}
