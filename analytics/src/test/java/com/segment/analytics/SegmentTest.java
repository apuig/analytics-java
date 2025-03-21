package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.segment.analytics.gson.AutoValueAdapterFactory;
import com.segment.analytics.gson.ISO8601DateAdapter;
import com.segment.analytics.messages.TrackMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import wiremock.com.fasterxml.jackson.core.JsonProcessingException;
import wiremock.com.fasterxml.jackson.databind.JsonNode;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;

public class SegmentTest {

    @Rule
    public WireMockRule wireMockRule =
            new WireMockRule(wireMockConfig().dynamicPort().gzipDisabled(true), false);

    Analytics analytics;

    GsonBuilder gsonBuilder = new GsonBuilder()
            .registerTypeAdapterFactory(new AutoValueAdapterFactory())
            .registerTypeAdapter(Date.class, new ISO8601DateAdapter());

    Gson gson = gsonBuilder.create();

    @Before
    public void confWireMock() {
        stubFor(post(urlEqualTo("/v1/import/")).willReturn(okJson("{\"success\": \"true\"}")));

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                .flushInterval(1, TimeUnit.SECONDS)
                .queueCapacity(500)
                // callback
                // http client
                .build();
    }

    @Test
    public void test() throws Throwable {
        analytics.enqueue(TrackMessage.builder("my-track").messageId("m1").userId("userId"));
        analytics.enqueue(TrackMessage.builder("my-track").messageId("m2").userId("userId"));

        Awaitility.await().until(() -> sentMessagesEqualsTo("m1", "m2"));
    }

    @Test
    public void testMore() throws Throwable {
        System.err.println("wm at " + wireMockRule.baseUrl());
        int num = 100_000;
        String[] expectedIds = new String[num];
        for (int i = 0; i < num; i++) {
            String id = "m" + i;
            expectedIds[i] = id;
            analytics.enqueue(TrackMessage.builder("my-track").messageId(id).userId("userId"));
        }

        Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> sentMessagesEqualsTo(expectedIds));
    }

    private static final ObjectMapper OM = new ObjectMapper();

    private boolean sentMessagesEqualsTo(String... msgIds) {
        return new HashSet<>(sentMessages()).equals(new HashSet<>(Arrays.asList(msgIds)));
    }

    private List<String> sentMessages() {
        List<String> messageIds = new ArrayList<>();
        for (ServeEvent event : wireMockRule.getAllServeEvents()) {
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
