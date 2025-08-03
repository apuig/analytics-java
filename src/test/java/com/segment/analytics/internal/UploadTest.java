package com.segment.analytics.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import dev.failsafe.CircuitBreaker;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.IdentifyMessage;
import com.segment.analytics.dto.TrackMessage;

public class UploadTest {

    @Rule
    public WireMockRule wireMock = new WireMockRule(wireMockConfig().dynamicPort());

    Upload up;
    List<Batch> fallbackBatches;
    Consumer<Batch> fallback;

    @Before
    public void setup() throws Throwable {
        fallbackBatches = new ArrayList<>();
        fallback = fallbackBatches::add;
        up = new Upload(
                HttpConfig.builder()
                        .circuitErrorsInAMinute(2)
                        .circuitRequestToClose(1)
                        .circuitSecondsInOpen(5)
                        .build(),
                new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH),
                fallback);
    }

    @Test
    public void uploadOk() {
        // Given a batch
        Batch b = batch();

        // and segment accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));

        // When upload
        up.upload(b);

        // Then the batch is received
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !wireMock.getAllServeEvents()
                .isEmpty());
        ServeEvent event =
                assertThat(wireMock.getAllServeEvents()).singleElement().actual();
        assertThat(event.getRequest().getBody()).isEqualTo(JSON.toJson(b));

        // And no fallback
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadKo() {
        // Given a batch
        Batch b = batch();

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
        Batch b = batch();

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(400).withBody("bad request")));

        // When upload
        up.upload(b);

        // Then the batch is discarted (no retry, no fallback)
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !wireMock.getAllServeEvents().isEmpty());
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadRateLimitOpensCircuit() throws Exception {
        // Given a batch
        Batch b = batch();

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
        CircuitBreaker<?> breaker = getBreaker(up);
        assertThat(breaker.isOpen()).isTrue();

        // Wait for circuit to half-open
        Thread.sleep(5000);
        assertThat(breaker.isHalfOpen() || breaker.isOpen()).isTrue();
    }

    private CircuitBreaker<?> getBreaker(Upload up) throws Exception {
        Field breakerField = Upload.class.getDeclaredField("breaker");
        breakerField.setAccessible(true);
        return (CircuitBreaker<?>) breakerField.get(up);
    }

    @Test
    public void uploadServerErrorRetries() {
        // Given a batch
        Batch b = batch();

        // and segment not accessible
        stubFor(post(urlEqualTo("/v1/b")).willReturn(aResponse().withStatus(500).withBody("server error")));

        // When upload
        up.upload(b);

        // Then the batch is received at the fallback
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> fallbackBatches.size() == 1);
        assertThat(fallbackBatches).singleElement().isEqualTo(b);
    }

    @Test
    public void uploadNetworkException()  {
        // Simulate network exception by shutting down WireMock
        wireMock.stop();
        Batch b = batch();
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
                        .build(),
                new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH),
                fallback);

        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));

        int batchCount = 10;
        CountDownLatch latch = new CountDownLatch(batchCount);
        for (int i = 0; i < batchCount; i++) {
            up.upload(batch());
            latch.countDown();
        }
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> wireMock.getAllServeEvents().size() == batchCount);
        assertThat(fallbackBatches).isEmpty();
    }

    @Test
    public void uploadGzipHeader() throws Exception {
        up = new Upload(
                HttpConfig.builder()
                        .gzip(true)
                        .build(),
                new URI(wireMock.baseUrl() + Defaults.DEFAULT_PATH),
                fallback);

        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));
        up.upload(batch());

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !wireMock.getAllServeEvents().isEmpty());
        ServeEvent event = wireMock.getAllServeEvents().get(0);
        assertThat(event.getRequest().getHeader("Content-Encoding")).isEqualTo("gzip");
    }

    @Test
    public void uploadConcurrent() throws Exception {
        stubFor(post(urlEqualTo("/v1/b")).willReturn(okJson("{\"success\": \"true\"}")));
        int batches = 20;
        CountDownLatch latch = new CountDownLatch(batches);
        AtomicInteger success = new AtomicInteger();
        for (int i = 0; i < batches; i++) {
            new Thread(() -> {
                up.upload(batch());
                success.incrementAndGet();
                latch.countDown();
            }).start();
        }
        latch.await(5, TimeUnit.SECONDS);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> wireMock.getAllServeEvents().size() == batches);
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
        Batch b = new Batch();
        b.setWriteKey("wk");
        b.setBatch(List.of(new IdentifyMessage(), new TrackMessage()));
        return b;
    }
}
