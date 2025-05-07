package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.TrackMessage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import wiremock.com.fasterxml.jackson.core.JsonProcessingException;
import wiremock.com.fasterxml.jackson.databind.JsonNode;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;
import wiremock.com.google.common.util.concurrent.RateLimiter;
import wiremock.org.apache.commons.lang3.RandomStringUtils;

@RunWith(JUnit4.class)
public class SegmentTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

    static int requestsPerSecond = 1_000;
    static int numClients = 10;
    static int messageContentChars = 100;
    static int responseDelay = 100;
    static final int DURATION_IN_SECONDS = 15;

    static Duration duration = Duration.ofSeconds(DURATION_IN_SECONDS);

    private Path tmpFolder;

    Analytics analytics;

    @Before
    public void setup() throws Exception {
        tmpFolder = Files.createTempDirectory("retryuploadtest");

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                // increase default flush size
                .httpConfig(HttpConfig.builder()
                        .size(1_000)
                        .flushSize(200)
                        .flushMs(5_000)
                        // configure circuit to check often
                        .circuitSecondsInOpen(10)
                        .circuitErrorsInAMinute(2)
                        .circuitRequestToClose(1)
                        .blockTimeout(0)
                        .gzip(true)
                        .build())
                .storageConfig(StorageConfig.builder()
                        .size(1_000)
                        .flushSize(200)
                        .flushMs(5_000)
                        .blockTimeout(5_000)
                        .filePath(tmpFolder.toString())
                        .build())
                .retryConfig(RetryConfig.builder()
                        .retryAt(List.of(
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(10),
                                Duration.ofSeconds(15),
                                Duration.ofSeconds(20),
                                Duration.ofSeconds(25),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(35),
                                Duration.ofSeconds(40),
                                Duration.ofSeconds(45),
                                Duration.ofSeconds(50),
                                Duration.ofSeconds(55),
                                Duration.ofSeconds(60),
                                Duration.ofSeconds(65)))
                        .delaySeconds(10)
                        .initialDelaySeconds(5)
                        .build())
                .build();
    }

    @Test
    public void testOk() throws Throwable {
        run(duration, new TimedAction(Duration.ZERO, this::segmentHttpUp));
    }

    @Test
    public void testFailRestoreAtEnd() throws Throwable {
        run(
                duration,
                new TimedAction(Duration.ZERO, this::segmentHttpDown),
                new TimedAction(duration, this::segmentHttpUp));
    }

    @Test
    public void testFailThenRestore() throws Throwable {
        run(
                duration,
                new TimedAction(Duration.ZERO, this::segmentHttpDown),
                new TimedAction(duration.dividedBy(2), this::segmentHttpUp));
    }

    private void run(final Duration durationToRun, final TimedAction... actions) throws Throwable {
        final long timeToRun = durationToRun.toMillis();
        final long start = System.currentTimeMillis();

        final String content = RandomStringUtils.randomAlphanumeric(messageContentChars);

        final AtomicInteger id = new AtomicInteger(0);
        final ExecutorService exec = new ThreadPoolExecutor(
                numClients,
                numClients,
                15L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(requestsPerSecond * 10),
                new CallerRunsPolicy());

        final RateLimiter rate = RateLimiter.create(requestsPerSecond);
        int actionIndex = 0;
        while (true) {
            final long elapsed = System.currentTimeMillis() - start;

            if ((actionIndex < actions.length) && (actions[actionIndex].at <= elapsed)) {
                actions[actionIndex].action.run();
                actionIndex++;
            }

            if (elapsed > timeToRun) {
                break;
            }

            if (rate.tryAcquire()) {
                exec.submit(() -> {
                    final String msgid = String.valueOf(id.getAndIncrement());

                    final TrackMessage msg = new TrackMessage("u", "e");
                    msg.setMessageId(msgid);
                    msg.setContext(Map.of("content", content));

                    analytics.enqueue(msg);
                });
            }
            Thread.yield();
        }
        exec.shutdown();
        if (!exec.awaitTermination(DURATION_IN_SECONDS * 5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("timeout waiting clients");
        }

        final int expectedEvents = id.get(); // only sending track events
        Awaitility.await()
                .atMost(DURATION_IN_SECONDS * 5, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> checkEventCount(expectedEvents));

        assertThat(countEventFiles()).isZero();
    }

    private long countEventFiles() throws IOException {
        return Files.list(tmpFolder).count();
    }

    void segmentHttpUp() {
        stubFor(post(urlEqualTo("/v1/b"))
                .willReturn(okJson("{\"success\": \"true\"}").withFixedDelay(responseDelay)));
        System.err.println("HTTP server UP");
    }

    void segmentHttpDown() {
        stubFor(post(urlEqualTo("/v1/b"))
                .willReturn(
                        WireMock.aResponse().withStatus(503).withBody("fail").withFixedDelay(responseDelay)));
        System.err.println("HTTP server DOWN");
    }

    private static final ObjectMapper OM = new ObjectMapper();

    private boolean checkEventCount(final int expected) {
        final int sentMessages = countSendMessages();
        System.err.println("Confirmed msgs %d / %d ".formatted(sentMessages, expected));
        return sentMessages == expected;
    }

    private int countSendMessages() {
        int count = 0;
        final Set<Integer> messageIds = new HashSet<>();
        for (final ServeEvent event : wireMockRule.getAllServeEvents()) {
            if (event.getResponse().getStatus() != 200) {
                continue;
            }

            JsonNode batch;
            try {
                final JsonNode json = OM.readTree(event.getRequest().getBodyAsString());
                batch = json.get("batch");
                if (batch == null) {
                    continue;
                }
            } catch (final JsonProcessingException e) {
                continue;
            }
            final Iterator<JsonNode> msgs = batch.elements();
            while (msgs.hasNext()) {
                count++;
                messageIds.add(msgs.next().get("messageId").asInt());
            }
        }
        if (count != messageIds.size()) {
            System.err.println(String.format("Duplicates!, count: %d messageIds: %d", count, messageIds.size()));
        }
        return messageIds.size();
    }

    @After
    public void tearDown() {
        analytics.close();
    }

    static class TimedAction {
        final long at;
        final Runnable action;

        public TimedAction(final Duration at, final Runnable run) {
            this.at = at.toMillis();
            this.action = run;
        }
    }
}
