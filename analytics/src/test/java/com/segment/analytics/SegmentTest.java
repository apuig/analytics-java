package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.messages.TrackMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
    public void confWireMockAndClient() {
        stubFor(post(urlEqualTo("/v1/import/")).willReturn(okJson("{\"success\": \"true\"}")));

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                .flushInterval(1, TimeUnit.SECONDS)
                .flushQueueSize(20)
                .queueCapacity(50)
                // http client
                .build();
    }

    @After
    public void tearDown() {
        analytics.shutdown();
    }

    @Test
    public void test() throws Throwable {

        stubFor(post(urlEqualTo("/v1/import/"))
                .willReturn(
                        WireMock.aResponse().withStatus(503).withBody("fail").withUniformRandomDelay(100, 1_000)));

        long start = System.currentTimeMillis();
        boolean upAgain = false;
        int id = 0;
        List<String> ids = new ArrayList<>();
        RateLimiter rate = RateLimiter.create(5);
        while (System.currentTimeMillis() - start < 60_000) {
            if (rate.tryAcquire()) {
                String msgid = "m" + id++;
                ids.add(msgid);
                analytics.enqueue(
                        TrackMessage.builder("my-track").messageId(msgid).userId("userId"));
                System.err.println("enqued " + msgid);
            }
            
            Thread.sleep(50);

            if (!upAgain && System.currentTimeMillis() - start > 20_000) {
                upAgain = true;
                stubFor(post(urlEqualTo("/v1/import/"))
                        .willReturn(okJson("{\"success\": \"true\"}").withUniformRandomDelay(100, 1_000)));
                System.err.println("UP AGAIN");
            }
        }

        Awaitility.await()
                .atMost(10, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> sentMessagesEqualsTo(ids.toArray(new String[ids.size()])));
    }

    private static final ObjectMapper OM = new ObjectMapper();

    private boolean sentMessagesEqualsTo(String... msgIds) {
        return new HashSet<>(sentMessages()).equals(new HashSet<>(Arrays.asList(msgIds)));
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
        System.err.println("Confirmed msgs : " + messageIds.size());
        return messageIds;
    }
}
