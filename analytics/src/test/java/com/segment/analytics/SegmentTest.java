package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.internal.AnalyticsClient;
import com.segment.analytics.internal.Config;
import com.segment.analytics.internal.Config.FileConfig;
import com.segment.analytics.internal.Config.HttpConfig;
import com.segment.analytics.internal.FallbackAppender;
import com.segment.analytics.messages.TrackMessage;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
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

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(
            wireMockConfig()
                    .port(8088)
                    // .dynamicPort()
                    .gzipDisabled(true),
            false);

    Analytics analytics;

    @Before
    public void confWireMockAndClient() throws IOException {
        FileUtils.deleteDirectory(Path.of(Config.DEFAULT_FALLBACK_FILE).toFile());

        stubFor(post(urlEqualTo("/v1/import/")).willReturn(okJson("{\"success\": \"true\"}")));

        // from SegmentQueue
        Integer SEGMENT_FLUSH_QUEUE_SIZE_DEFAULT = 50;
	Integer SEGMENT_QUEUE_SIZE_DEFAULT = 250;
        Integer DEFAULT_FLUSH_PERIOD_IN_SECONDS = 10;

        // from SegmentQueue getBoundedNetworkExecutor
        Integer SEGMENT_EXECUTOR_QUEU_SIZE_DEFAULT = 0; // 5;
        Integer EXECUTOR_SIZE = 1;
        Integer EXECUTOR_KEEPALIVE_SECONDS = 15;

        ThreadPoolExecutor boundedNetworkExecutor = new ThreadPoolExecutor(
                EXECUTOR_SIZE, // corePoolSize
                EXECUTOR_SIZE, // maximumPoolSize
                EXECUTOR_KEEPALIVE_SECONDS, // keepAliveTime
                TimeUnit.SECONDS,
                SEGMENT_EXECUTOR_QUEU_SIZE_DEFAULT == 0
                        ? new SynchronousQueue<>(true)
                        : new ArrayBlockingQueue<>(SEGMENT_EXECUTOR_QUEU_SIZE_DEFAULT, true),
                // this will cause the HTTP requests to be handled on AnalyticsClient.Looper
                // SegmentQueue was discarding oldest tasks // e.getQueue().poll(); e.execute(r);
                new CallerRunsPolicy() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        // System.err.println("==== Pool exhausted ====");
                        super.rejectedExecution(r, e);
                    }
                    ;
                });

        // segment Platform getDefaultClient
        Integer HTTP_TIMEOUT_SECONDS = 15;
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .readTimeout(HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .writeTimeout(HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                // addedd bounded executor
                .dispatcher(new Dispatcher(boundedNetworkExecutor))
                .build();

        // Then it always add the user-agent interceptor
        // FIXME not forcing tls

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                .client(client)
                .networkExecutor(boundedNetworkExecutor)
                .httpConfig(HttpConfig.builder()
                        .flushQueueSize(SEGMENT_FLUSH_QUEUE_SIZE_DEFAULT)
                        .queueSize(SEGMENT_QUEUE_SIZE_DEFAULT)
                        .flushIntervalInMillis(DEFAULT_FLUSH_PERIOD_IN_SECONDS * 1_000)
                        .build())
                .fileConfig(FileConfig.builder().build())
                .build();
    }

    @After
    public void tearDown() {
        analytics.close();
    }

    static void configLogger() {
        ConsoleHandler console = new ConsoleHandler();
        console.setLevel(Level.ALL);
        console.setFormatter(new SimpleFormatter() {
            @Override
            public String format(LogRecord record) {
                return String.format("[%1$tT.%1$tL] %2$s %n", record.getMillis(), record.getMessage());
            }
        });

        Logger l1 = Logger.getLogger(FallbackAppender.class.getName());
        Logger l2 = Logger.getLogger(AnalyticsClient.class.getName());
        l1.setLevel(Level.ALL);
        l1.addHandler(console);
        l2.setLevel(Level.ALL);
        l2.addHandler(console);
    }

    @Test
    public void test() throws Throwable {
        configLogger();

	int requestsPerSecond = 1_000;
        int numClients = 10;
	int messageContentChars = 100;

	int timeToRun = 60_000 * 2;
	int timeToRestore = 60_000 * 1;

	int responseDelay = 300;

        stubFor(post(urlEqualTo("/v1/import/"))
                .willReturn(
                        WireMock.aResponse().withStatus(503).withBody("fail").withFixedDelay(responseDelay)));

        long start = System.currentTimeMillis();
        boolean upAgain = false;
        final AtomicInteger id = new AtomicInteger(0);
        List<String> ids = new ArrayList<>();

        RateLimiter rate = RateLimiter.create(requestsPerSecond);
        ExecutorService exec = Executors.newWorkStealingPool(numClients);

        while (System.currentTimeMillis() - start < timeToRun) {
            if (rate.tryAcquire()) {
                exec.submit(() -> {
                    String msgid = "m" + id.getAndIncrement();
                    ids.add(msgid);
                    analytics.enqueue(
		      TrackMessage.builder("my-track").messageId(msgid).userId("userId")
			  .context(Map.of("content", RandomStringUtils.randomAlphanumeric(messageContentChars))));
                });
            }

            if (!upAgain && System.currentTimeMillis() - start > timeToRestore) {
                upAgain = true;
                stubFor(post(urlEqualTo("/v1/import/"))
                        .willReturn(okJson("{\"success\": \"true\"}").withFixedDelay(responseDelay)));
                System.err.println("UP AGAIN");
            }
        }

        Awaitility.await()
                .atMost(10, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> sentMessagesEqualsTo(ids.toArray(new String[ids.size()])));

        exec.shutdownNow();
        exec.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static final ObjectMapper OM = new ObjectMapper();

    private boolean sentMessagesEqualsTo(String... msgIds) {
        Set<String> sentMessages = sentMessages();
        System.err.println("Confirmed msgs %d / %d ".formatted(sentMessages.size(), msgIds.length));
        return sentMessages().equals(new HashSet<>(Arrays.asList(msgIds)));
    }

    private Set<String> sentMessages() {
        Set<String> messageIds = new HashSet<>();
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
                messageIds.add(msgs.next().get("messageId").asText());
            }
        }
        return messageIds;
    }
}
