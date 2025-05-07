package com.segment.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.segment.analytics.Analytics.Builder;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.TrackMessage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AnalyticsTest {

    private Analytics analytics;

    @Before
    public void setup() throws Exception {
        // Clean up storage before each test
        final Path storagePath = Files.createTempDirectory("segment-test");
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
    public void enqueueThrowsOnNullMessageId() {
        final TrackMessage msg = new TrackMessage("u", "e");
        msg.setMessageId(null);
        // messageId not set
        assertThatThrownBy(() -> analytics.enqueue(msg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Message without messageId");
    }

    @Test
    public void enqueueAfterShutdownDoesNotThrow() {
        analytics.close();
        final TrackMessage msg = new TrackMessage("u", "e");
        analytics.enqueue(msg);
        // Should not throw
        assertTrue(true);
    }

    @Test
    public void enqueueOversizedMessageThrows() {
        final TrackMessage msg = new TrackMessage("u", "e");
        // Add large context to exceed MSG_MAX_SIZE
        msg.setContext(java.util.Map.of("big", "x".repeat(200_000)));
        assertThatThrownBy(() -> analytics.enqueue(msg))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Message was above individual limit");
    }

    @Test
    public void storageQueueUseOfferDropsWhenFull() throws Exception {
        // Use small queue sizes for test
        try (Analytics analyticsQueue1 = Analytics.builder("test-write-key")
                .endpoint("http://localhost:8080")
                .httpConfig(HttpConfig.builder().size(1).build())
                // And not blocking on storage
                .storageConfig(StorageConfig.builder().size(1).blockTimeout(0).build())
                .build()) {

            // Fill uploadQueue
            analyticsQueue1.enqueue(new TrackMessage("u", "e"));
            // Fill storageQueue by overflowing uploadQueue
            analyticsQueue1.enqueue(new TrackMessage("u", "e"));
            // Now both queues are full, next enqueue should not block and should log a lost event
            final TestLogHandler handler = new TestLogHandler();
            final Logger logger = Logger.getLogger(Analytics.class.getName());
            logger.addHandler(handler);

            analyticsQueue1.enqueue(new TrackMessage("u", "e"));

            logger.removeHandler(handler);

            assertThat(handler.records)
                    .anyMatch(r -> (r.getLevel().intValue() >= Level.WARNING.intValue())
                            && r.getMessage().contains("Lost overflow event"));
        }
    }

    @Test
    public void storageQueuePutBlocksWhenFull() throws Exception {
        // Use small queue sizes for test
        analytics.close();
        analytics = Analytics.builder("test-write-key")
                .endpoint("http://localhost:8080")
                .httpConfig(HttpConfig.builder().size(1).flushMs(60_000).build())
                .storageConfig(StorageConfig.builder()
                        .size(1)
                        .flushMs(60_000)
                        .blockTimeout(10_000)
                        .build())
                .build();
        // And prevent the events from being consumed
        analytics.storageQueue.close();
        analytics.uploadQueue.close();

        // Fill uploadQueue
        analytics.enqueue(new TrackMessage("u", "e"));

        // Fill storageQueue by overflowing uploadQueue
        analytics.enqueue(new TrackMessage("u", "e"));

        // Now both queues are full, next enqueue should block.
        // We'll test that it blocks by running in another thread and timing out.
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread t = new Thread(() -> {
            try {
                analytics.enqueue(new TrackMessage("u", "e"));
            } catch (final Exception ignored) {
                // no-op
            }
            latch.countDown();
        });
        t.start();

        // Wait a short time to see if latch is still not counted down (i.e., thread is blocked)
        // Should still be blocked
        assertThat(latch.await(200, TimeUnit.MILLISECONDS)).isFalse();

        t.interrupt(); // Clean up
        analytics.close();
    }

    @Test
    public void builderThrowsOnInvalidWriteKey() {
        assertThatThrownBy(() -> Analytics.builder(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Analytics.builder("")).isInstanceOf(IllegalArgumentException.class);
        final String shortKey = "a".repeat(33);
        assertThatThrownBy(() -> Analytics.builder(shortKey)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void builderThrowsOnInvalidEndpoint() {
        final Builder b = Analytics.builder("test-write-key");
        assertThatThrownBy(() -> b.endpoint(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.endpoint("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void builderWithDefaults() throws IOException {
        assertNotNull(Analytics.builder("test-write-key").build());
    }
}
