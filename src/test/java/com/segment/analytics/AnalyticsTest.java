package com.segment.analytics;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.segment.analytics.config.Defaults;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.TrackMessage;

public class AnalyticsTest {

    private Analytics analytics;

    @Before
    public void setup() throws Exception {
        // Clean up storage before each test
        Path storagePath = Path.of(Defaults.DEFAULT_STORAGE_FILE);
        if (storagePath.toFile().exists()) {
            storagePath.toFile().delete();
        }
        analytics = Analytics.builder("test-write-key")
                .endpoint("http://localhost:8080")
                .httpConfig(HttpConfig.builder().build())
                .storageConfig(StorageConfig.builder().build())
                .build();
    }

    @After
    public void tearDown() {
        analytics.close();
    }

    @Test
    public void builderThrowsOnInvalidWriteKey() {
        assertThatThrownBy(() -> Analytics.builder(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Analytics.builder(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Analytics.builder("a".repeat(33)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void enqueueThrowsOnNullMessageId() {
        TrackMessage msg = new TrackMessage();
        msg.setUserId("user");
        // messageId not set
        assertThatThrownBy(() -> analytics.enqueue(msg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Message without messageId");
    }

    @Test
    public void enqueueAfterShutdownDoesNotThrow() {
        analytics.close();
        TrackMessage msg = new TrackMessage();
        msg.setMessageId(UUID.randomUUID().toString());
        msg.setUserId("user");
        analytics.enqueue(msg); 
        // Should not throw, just log warning
    }

    @Test
    public void enqueueOversizedMessageThrows() {
        TrackMessage msg = new TrackMessage();
        msg.setMessageId(UUID.randomUUID().toString());
        msg.setUserId("user");
        // Add large context to exceed MSG_MAX_SIZE
        msg.setContext(java.util.Map.of("big", "x".repeat(200_000)));
        assertThatThrownBy(() -> analytics.enqueue(msg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Message was above individual limit");
    }
}
