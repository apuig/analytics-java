package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.TrackMessage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StorageTest {

    Path tmpFolder;
    StorageConfig config;

    @Before
    public void setup() throws IOException {
        tmpFolder = Files.createTempDirectory("storagetest");
        config = StorageConfig.builder().filePath(tmpFolder.toString()).build();
    }

    @Test
    public void initAndRetryAfter() throws Throwable {
        // Given a configured initial delay
        Duration initialDelay = Duration.ofSeconds(10);
        Storage s =
                new Storage(RetryConfig.builder().retryAt(List.of(initialDelay)).build(), config);

        // When create a fileName for the first retry
        Instant now = Instant.now();
        Batch b = new Batch();
        b.setSentAt(now);
        b.setWriteKey("wk");
        b.setBatch(List.of(new TrackMessage()));

        s.write(b);

        String fileName = assertThat(tmpFolder.toFile().listFiles())
                .singleElement()
                .actual()
                .getName();

        // Then the create fileName contains information to respect the initial retryAfter
        assertThat(fileName).startsWith(String.valueOf(now.toEpochMilli()));
        assertThat(fileName).contains("_%d_".formatted(now.toEpochMilli() + initialDelay.toMillis()));
        assertThat(fileName).contains("_0_");
    }

    @Test
    public void handleRetry() throws IOException {
        // Given a retry sequence with 100ms and 1s
        Duration secondDelay = Duration.ofSeconds(1);
        Storage s = new Storage(
                RetryConfig.builder()
                        .retryAt(List.of(Duration.ofMillis(100), secondDelay))
                        .build(),
                config);

        // And a file initialized with the expected format
        Instant now = Instant.now();
        Batch b = new Batch();
        b.setSentAt(now);
        b.setWriteKey("wk");
        b.setBatch(List.of(new TrackMessage()));
        s.write(b);
        String fileName = assertThat(tmpFolder.toFile().listFiles())
                .singleElement()
                .actual()
                .getName();

        Path path = s.tryMoveToTmp(tmpFolder.resolve(fileName));

        // When handleRetry
        s.handleRetry(path);

        // Then the file was moved and it updated the retry and retry-after
        assertThat(path.toFile().exists()).isFalse();
        File[] files = tmpFolder.toFile().listFiles();
        File file = assertThat(files).singleElement().actual();
        String[] newFile = file.getName().split("_");
        String[] original = fileName.split("_");
        assertEquals(original[0], newFile[0]);
        assertEquals(Long.parseLong(original[0]) + secondDelay.toMillis(), Long.parseLong(newFile[1]));
        assertEquals(Long.parseLong(original[2]) + 1, Long.parseLong(newFile[2]));
        assertEquals(original[3], newFile[3]);
    }

    @Test
    public void handleRetryLast() throws Throwable {
        // Given a retry sequence of only one element
        Storage s = new Storage(
                RetryConfig.builder().retryAt(List.of(Duration.ofMillis(100))).build(), config);

        // And a file initialized with the expected format
        Instant now = Instant.now();
        Batch b = new Batch();
        b.setSentAt(now);
        b.setWriteKey("wk");
        b.setBatch(List.of(new TrackMessage()));
        s.write(b);
        String fileName = assertThat(tmpFolder.toFile().listFiles())
                .singleElement()
                .actual()
                .getName();

        Path path = s.tryMoveToTmp(tmpFolder.resolve(fileName));

        // When handleRetry
        s.handleRetry(path);

        // Then the file is deleted
        assertThat(path.toFile().exists()).isFalse();
        // And no more files are created
        assertThat(tmpFolder.toFile().listFiles()).isEmpty();
    }
}
