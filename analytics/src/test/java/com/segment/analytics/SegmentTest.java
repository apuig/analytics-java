package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.internal.Config;
import com.segment.analytics.internal.Config.FileConfig;
import com.segment.analytics.internal.Config.HttpConfig;
import com.segment.analytics.messages.TrackMessage;
import java.io.IOException;
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
import org.apache.commons.lang.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import wiremock.com.fasterxml.jackson.core.JsonProcessingException;
import wiremock.com.fasterxml.jackson.databind.JsonNode;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;
import wiremock.com.google.common.util.concurrent.RateLimiter;

public class SegmentTest {

    public int requestsPerSecond = 1_000;
    public int numClients = 10;
    public int messageContentChars = 100;
    public int responseDelay = 300;

    @Rule
    public WireMockRule wireMockRule =
            new WireMockRule(wireMockConfig().port(8088).gzipDisabled(true), false);

    Analytics analytics;

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(Path.of(Config.DEFAULT_FALLBACK_FILE).toFile());

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                .httpConfig(HttpConfig.builder()
                        // .queueSize(250)
                        // .flushQueueSize(50)
                        // .flushIntervalInMillis(10 * 1_000)
                        // .executorSize(1)
                        // .executorQueueSize(0)
                        // .timeoutSeconds(15)
                        .build())
                .fileConfig(FileConfig.builder()
                        // .size(250)
                        // .flushSize(50)
                        .build())
                .build();
    }

   
    @Test
    public void testOk() throws Throwable {
        run(Duration.ofSeconds(30), new TimedAction(Duration.ZERO, () -> segmentHttpUp()));
    }

    @Test
    public void testFailRestoreAtEnd() throws Throwable {
        Duration duration = Duration.ofSeconds(30);
        run(
                duration,
                new TimedAction(Duration.ZERO, () -> segmentHttpDown()),
                new TimedAction(duration, () -> segmentHttpUp()));
    }

    @Test
    public void testFailThenRestore() throws Throwable {
        run(
                Duration.ofMinutes(2),
                new TimedAction(Duration.ZERO, () -> segmentHttpDown()),
                new TimedAction(Duration.ofMinutes(1), () -> segmentHttpUp()));
    }

    private void run(Duration durationToRun, TimedAction... actions) throws Throwable {
        long timeToRun = durationToRun.toMillis();
        long start = System.currentTimeMillis();

        final String content = RandomStringUtils.randomAlphanumeric(messageContentChars);
        final AtomicInteger id = new AtomicInteger(0);

        ExecutorService exec = new ThreadPoolExecutor(
                numClients,
                numClients,
                15l,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(10_000),
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
                    analytics.enqueue(TrackMessage.builder("my-track")
                            .messageId(msgid)
                            .userId("userId")
                            .context(Map.of("content", content)));
                });
            }
            Thread.yield();
        }

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.MINUTES);

        Awaitility.await()
                .atMost(10, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> checkSentMessages(id.get()));
    }

    void segmentHttpUp() {
        stubFor(post(urlEqualTo("/v1/import/"))
                .willReturn(okJson("{\"success\": \"true\"}").withFixedDelay(responseDelay)));
        System.err.println("HTTP server UP");
    }

    void segmentHttpDown() {
        stubFor(post(urlEqualTo("/v1/import/"))
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
