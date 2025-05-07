package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.TrackMessage;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
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
    public WireMockRule wireMockRule =
            new WireMockRule(wireMockConfig().port(8088).gzipDisabled(true), false);

    static int requestsPerSecond = 1_000;
    static int numClients = 10;
    static int messageContentChars = 100;
    static int responseDelay = 100;
    static final int durationInSeconds = 60;

    static Duration duration = Duration.ofSeconds(durationInSeconds);

    Analytics analytics;

    public static void main(String[] args) throws MalformedURLException, IOException {
        Analytics analytics = Analytics.builder("write-key")
                .endpoint("http://localhost:8080")
                .httpConfig(HttpConfig.builder()
                        .build())
                .storageConfig(StorageConfig.builder()
                        .build())
                .build();
        TrackMessage msg = new TrackMessage();
        msg.setEvent("my-track");
        msg.setMessageId("a");
        msg.setUserId("userId");

        analytics.enqueue(msg);
    }

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(Path.of(Defaults.DEFAULT_STORAGE_FILE).toFile());

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                .build();
    }

    @Test
    public void testOk() throws Throwable {
        run(duration, new TimedAction(Duration.ZERO, () -> segmentHttpUp()));
    }

    @Test
    public void testFailRestoreAtEnd() throws Throwable {
        run(
                duration,
                new TimedAction(Duration.ZERO, () -> segmentHttpDown()),
                new TimedAction(duration, () -> segmentHttpUp()));
    }

    @Test
    public void testFailThenRestore() throws Throwable {
        run(
                duration,
                new TimedAction(Duration.ZERO, () -> segmentHttpDown()),
                new TimedAction(duration.dividedBy(2), () -> segmentHttpUp()));
    }

    private void run(Duration durationToRun, TimedAction... actions) throws Throwable {
        long timeToRun = durationToRun.toMillis();
        long start = System.currentTimeMillis();

        String content = RandomStringUtils.randomAlphanumeric(messageContentChars);
        long lastActivity = System.currentTimeMillis();

        final AtomicInteger id = new AtomicInteger(0);
        ExecutorService exec = new ThreadPoolExecutor(
                numClients,
                numClients,
                15l,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(requestsPerSecond * 10),
                new CallerRunsPolicy());

        RateLimiter rate = RateLimiter.create(requestsPerSecond);
        int actionIndex = 0;
        while (true) {
            long elapsed = System.currentTimeMillis() - start;

            if (actionIndex < actions.length && actions[actionIndex].at <= elapsed) {
                actions[actionIndex].action.run();
                actionIndex++;
            }

            if (elapsed > timeToRun) {
                break;
            }

            if (rate.tryAcquire()) {
                exec.submit(() -> {
                    String msgid = String.valueOf(id.getAndIncrement());

                    TrackMessage msg = new TrackMessage();
                    msg.setEvent("my-track");
                    msg.setMessageId(msgid);
                    msg.setUserId("userId");
                    msg.setContext(Map.of("content", content));

                    analytics.enqueue(msg);
                });
            }
            Thread.yield();
        }
        exec.shutdown();
        if (!exec.awaitTermination(durationInSeconds * 5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("timeout waiting clients");
        }

        int expectedEvents = id.get();
        Awaitility.await()
                .atMost(durationInSeconds * 5, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> checkSentMessages(expectedEvents));

        // FIXME assertThat(countEventFiles()).isZero();
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

    private boolean checkSentMessages(int expected) {
        int sentMessages = countSendMessages();
        System.err.println("Confirmed msgs %d / %d ".formatted(sentMessages, expected));
        return sentMessages >= expected;
    }

    private int countSendMessages() {
        int count = 0;
        Set<Integer> messageIds = new HashSet<>();
        for (ServeEvent event : wireMockRule.getAllServeEvents()) {
            if (event.getResponse().getStatus() != 200) {
                continue;
            }

            JsonNode batch;
            try {
                JsonNode json = OM.readTree(event.getRequest().getBodyAsString());
                batch = json.get("batch");
                if (batch == null) {
                    continue;
                }
            } catch (JsonProcessingException e) {
                continue;
            }
            Iterator<JsonNode> msgs = batch.elements();
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

        public TimedAction(Duration at, Runnable run) {
            super();
            this.at = at.toMillis();
            this.action = run;
        }
    }
}
