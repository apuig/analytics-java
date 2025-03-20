package com.segment.analytics;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.segment.analytics.messages.TrackMessage;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class SegmentTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort(), false);

    Analytics analytics;

    @Before
    public void confWireMock() {
        stubFor(post(urlEqualTo("/v1/import/")).willReturn(okJson("{\"success\": \"true\"}")));

        analytics = Analytics.builder("write-key")
                .endpoint(wireMockRule.baseUrl())
                // callback
                // http client
                .build();
    }

    @Test
    public void test() {
        analytics.enqueue(TrackMessage.builder("my-track")
                .messageId(UUID.randomUUID().toString())
                .userId("userId"));
    }
}
