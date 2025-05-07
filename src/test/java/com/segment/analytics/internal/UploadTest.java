package com.segment.analytics.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.TestLogHandler;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.IdentifyMessage;
import com.segment.analytics.dto.TrackMessage;
import dev.failsafe.CircuitBreaker;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class UploadTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    Upload up;
    List<Batch> fallbackBatches;
    BatchConsumer<Batch> fallback;

    @Before
    public void setup() throws Throwable {
        fallbackBatches = new ArrayList<>();
        fallback = fallbackBatches::add;
        up = new Upload(
                HttpConfig.builder()
                        .circuitErrorsInAMinute(2)
                        .circuitRequestToClose(1)
                        .circuitSecondsInOpen(5)
                        .gzip(false)
                        .build(),
                new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH),
                fallback);
    }

    @After
    public void tearDown() {
        wireMock.resetRequests();
        up.close();
    }

    @Test
    public void uploadOk() {
        // Given a batch
        final Batch b = batch();

        // and segment accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));

        // When upload
        up.upload(b);

        // Then the batch is received
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !wireMock.getAllServeEvents()
                .isEmpty());
        final ServeEvent event =
                assertThat(wireMock.getAllServeEvents()).singleElement().actual();
        assertThat(event.getRequest().getBody()).isEqualTo(JSON.toJson(b));

        // And no fallback
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadKo() {
        // Given a batch
        final Batch b = batch();

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(503).withBody("fail")));

        // When upload
        up.upload(b);

        // Then the batch is received at the fallback
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !fallbackBatches.isEmpty());
        assertThat(fallbackBatches).singleElement().isEqualTo(b);
    }

    @Test
    public void uploadBadRequestNoRetry() {
        // Given a batch
        final Batch b = batch();

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(400).withBody("bad request")));

        // When upload
        up.upload(b);

        // Then the batch is discarted (no retry, no fallback)
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !wireMock.getAllServeEvents()
                .isEmpty());
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadRateLimitOpensCircuit() throws Exception {
        // Given a batch
        final Batch b = batch();

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(429).withBody("rate limit")));

        // When upload
        up.upload(b);

        // Then the batch is received at the fallback
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !fallbackBatches.isEmpty());
        assertThat(fallbackBatches).singleElement().isEqualTo(b);

        // Circuit should be open after rate limit
        up.upload(batch());
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> fallbackBatches.size() == 2);

        // Check circuit breaker state is OPEN
        final CircuitBreaker<?> breaker = getBreaker(up);
        assertThat(breaker.isOpen()).isTrue();

        // Wait for circuit to half-open
        Thread.sleep(5000);
        assertThat(breaker.isHalfOpen() || breaker.isOpen()).isTrue();
    }

    private CircuitBreaker<?> getBreaker(final Upload up) throws Exception {
        final Field breakerField = Upload.class.getDeclaredField("breaker");
        breakerField.setAccessible(true);
        return (CircuitBreaker<?>) breakerField.get(up);
    }

    @Test
    public void uploadServerErrorRetries() {
        // Given a batch
        final Batch b = batch();

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(500).withBody("server error")));

        // When upload
        up.upload(b);

        // Then the batch is received at the fallback
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> fallbackBatches.size() == 1);
        assertThat(fallbackBatches).singleElement().isEqualTo(b);
    }

    @Test
    public void uploadNetworkException() {
        // Simulate network exception by shutting down WireMock
        wireMock.stop();
        final Batch b = batch();
        up.upload(b);

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> fallbackBatches.size() == 1);
        wireMock.start();
    }

    @Test
    public void uploadExecutorExhaustion() throws Exception {
        // Create Upload with small executor queue
        up = new Upload(
                HttpConfig.builder()
                        .executorSize(1)
                        .executorQueueSize(0)
                        .gzip(false)
                        .build(),
                new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH),
                fallback);

        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));

        final int batchCount = 10;
        final CountDownLatch latch = new CountDownLatch(batchCount);
        for (int i = 0; i < batchCount; i++) {
            up.upload(batch());
            latch.countDown();
        }
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> wireMock.getAllServeEvents().size() == batchCount);
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadGzipHeader() throws Exception {
        up.close();
        up = new Upload(
                HttpConfig.builder().gzip(true).build(), new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH), fallback);

        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));
        final Batch b = batch();
        up.upload(b);

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !wireMock.getAllServeEvents()
                .isEmpty());
        final ServeEvent event = wireMock.getAllServeEvents().get(0);
        assertThat(event.getRequest().getHeader("Content-Encoding")).isEqualTo("gzip");

        // wiremock already ungzip it
        final byte[] body = event.getRequest().getBody();
        assertThat(body).isEqualTo(JSON.toJson(b));
    }

    @Test
    public void uploadConcurrent() throws Exception {
        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));
        final int batches = 20;
        final CountDownLatch latch = new CountDownLatch(batches);
        final AtomicInteger success = new AtomicInteger();
        for (int i = 0; i < batches; i++) {
            new Thread(() -> {
                        up.upload(batch());
                        success.incrementAndGet();
                        latch.countDown();
                    })
                    .start();
        }
        latch.await(5, TimeUnit.SECONDS);
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> wireMock.getAllServeEvents().size() == batches);
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadCircuit() throws Throwable {
        // Given circuit configuration

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(503).withBody("fail")));

        // When 2 upload
        up.upload(batch());
        up.upload(batch());

        // Then the 2 batch arrives at the destination and fails
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> receivedCount() == 2);
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> fallbackBatches.size() == 2);

        // When another upload attempt (OPEN state)
        up.upload(batch());

        // Then the batch is not received at the destination
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> fallbackBatches.size() == 3);
        assertThat(receivedCount()).isEqualTo(2);

        // When half open
        Thread.sleep(5_000);
        // and segment accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));

        // When upload
        up.upload(batch());

        // Then the batch is received
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> receivedCount() == 3);
        assertThat(fallbackBatches).hasSize(3);
    }

    private int receivedCount() {
        return wireMock.getAllServeEvents().size();
    }

    private static Batch batch() {
        final Batch b = new Batch();
        b.setWriteKey("wk");
        b.setBatch(List.of(new IdentifyMessage("u", null), new TrackMessage("u", "e")));
        return b;
    }

    @Test
    public void retryHandlesFileNotFoundException() {

        assertThatCode(() -> {
                    up.retry(
                            // Given a path that does not exist
                            // When retry is called with a non-existent file
                            Paths.get("/tmp/this_file_should_not_exist_123456789"),
                            p -> {
                                throw new RuntimeException("should not be called");
                            },
                            p -> {
                                throw new RuntimeException("should not be called");
                            });
                })
                // Then: No exception is thrown, error is logged
                .doesNotThrowAnyException();
    }

    @Test
    public void retryWithInvalidFileRetry() throws Exception {
        // Given: a temp file with invalid (unreadable) permissions
        up.close();
        up = new Upload(
                HttpConfig.builder().gzip(true).build(), new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH), fallback);

        final Path tmp = Files.createTempFile("segment-upload-test", ".tmp");
        Files.writeString(tmp, "foo");
        tmp.toFile().setReadable(false, false);

        // Setup log handler to capture logs
        final TestLogHandler handler = new TestLogHandler();
        final Logger logger = Logger.getLogger(Upload.class.getName());
        logger.addHandler(handler);

        final AtomicBoolean noRetry = new AtomicBoolean();
        final AtomicBoolean retry = new AtomicBoolean();
        // When: retry is called
        assertThatCode(() -> {
                    up.retry(
                            tmp,
                            p -> {
                                noRetry.set(true);
                            },
                            p -> {
                                retry.set(true);
                            });
                })
                .doesNotThrowAnyException();

        assertThat(retry.get()).isTrue();
        assertThat(noRetry.get()).isFalse();
        // Then: log contains "Failed to gzip payload from file"
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(handler.records.stream()
                            .map(LogRecord::getMessage)
                            .anyMatch(msg -> (msg != null) && msg.contains("Failed to read for gzip from file")))
                    .isTrue();
        });

        logger.removeHandler(handler);
    }

    @Test
    public void uploadRejectedExecution() throws Exception {
        // Given a custom Upload with executorSize=1, queueSize=0 (SynchronousQueue)
        // and a blocking fallback to ensure the thread pool is exhausted

        final TestLogHandler handler = new TestLogHandler();
        final Logger logger = Logger.getLogger(Upload.class.getName());
        logger.addHandler(handler);

        up = new Upload(
                HttpConfig.builder().executorSize(1).executorQueueSize(0).build(),
                new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH),
                fallback);

        stubFor(post(urlEqualTo("/v1/b"))
                .willReturn(okJson("{\"success\": \"true\"}").withFixedDelay(1_000)));

        // Submit one batch to occupy the only thread
        up.upload(batch());
        // Submit another batch, which should trigger rejectedExecution and run in caller thread
        up.upload(batch());

        // Await for both requests to be processed
        await().atMost(Duration.ofSeconds(3))
                .until(() -> wireMock.getAllServeEvents().size() == 2);

        // Check that the log contains the rejectedExecution message
        assertThat(handler.records)
                .anyMatch(r -> (r.getLevel().intValue() >= Level.FINE.intValue())
                        && r.getMessage().contains("networkPool exhausted"));
        logger.removeHandler(handler);
    }
}
