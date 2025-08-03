package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.TrackMessage;

public class RetryUploadTest {

    private Path tmpFolder;
    private Storage storage;
    private RetryUpload retryUpload;
    private AtomicBoolean uploadCalled;
    private AtomicBoolean deleteCalled;
    private AtomicBoolean retryCalled;

    @Before
    public void setup() throws IOException {
        tmpFolder = Files.createTempDirectory("retryuploadtest");
        StorageConfig config =
                StorageConfig.builder().filePath(tmpFolder.toString()).build();
        storage = new Storage(
                RetryConfig.builder()
                        .initialDelaySeconds(0)
                        .delaySeconds(1)
                        .retryAt(List.of(Duration.ofMillis(10), Duration.ofMinutes(1)))
                        .build(),
                config);

        uploadCalled = new AtomicBoolean(false);
        deleteCalled = new AtomicBoolean(false);
        retryCalled = new AtomicBoolean(false);
    }

    @After
    public void cleanup() throws IOException {
        if (retryUpload != null) {
            retryUpload.close();
        }
        Files.walk(tmpFolder).map(Path::toFile).forEach(File::delete);
    }

    private RetryUpload createRetryUpload(RetryConfig retryConfig, Upload upload) {
        return new RetryUpload(Thread::new, retryConfig, storage, upload);
    }

    private Batch createBatch() {
        Batch b = new Batch();
        b.setSentAt(Instant.now());
        b.setWriteKey("wk");
        b.setBatch(List.of(new TrackMessage()));
        return b;
    }

    @Test
    public void retryDeletesFileOnSuccess() throws Exception {
        storage.write(createBatch());

        Upload upload = new Upload(HttpConfig.builder().build(), URI.create("http://localhost"), batch -> {}) {
            @Override
            public void retry(Path file, Consumer<Path> noRetry, Consumer<Path> retry) {
                uploadCalled.set(true);
                noRetry.accept(file);
                deleteCalled.set(true);
            }
        };

        retryUpload = createRetryUpload(
                RetryConfig.builder()
                        .initialDelaySeconds(0)
                        .delaySeconds(1)
                        .retryAt(List.of(Duration.ofMillis(10)))
                        .build(),
                upload);

        Thread.sleep(1_200); // Allow scheduled run
        assertThat(uploadCalled.get()).isTrue();
        assertThat(deleteCalled.get()).isTrue();
        assertThat(tmpFolder.toFile().listFiles()).isEmpty();
    }

    @Test
    public void retryCallsHandleRetryOnFailure() throws Exception {
        storage.write(createBatch());

        Upload upload = new Upload(HttpConfig.builder().build(), URI.create("http://localhost"), batch -> {}) {
            @Override
            public void retry(Path file, Consumer<Path> noRetry, Consumer<Path> retry) {
                uploadCalled.set(true);
                retry.accept(file);
                retryCalled.set(true);
            }
        };

        retryUpload = createRetryUpload(
                RetryConfig.builder()
                        .initialDelaySeconds(0)
                        .delaySeconds(1)
                        .retryAt(List.of(Duration.ofMillis(10), Duration.ofMinutes(1)))
                        .build(),
                upload);

        Thread.sleep(1_200); // Allow scheduled run

        assertThat(uploadCalled.get()).isTrue();
        assertThat(retryCalled.get()).isTrue();
        assertThat(tmpFolder.toFile().listFiles()).hasSize(1);
    }

    @Test
    public void closeShutsDownExecutor() throws Exception {
        Upload upload = new Upload(HttpConfig.builder().build(), URI.create("http://localhost"), batch -> {}) {
            @Override
            public void retry(Path file, Consumer<Path> noRetry, Consumer<Path> retry) {
                // no-op
            }
        };
        retryUpload = createRetryUpload(
                RetryConfig.builder()
                        .initialDelaySeconds(0)
                        .delaySeconds(1)
                        .retryAt(List.of(Duration.ofMillis(10)))
                        .build(),
                upload);
        retryUpload.close();
    }

    @Test
    public void runWithNoFilesDoesNothing() throws Exception {
        Upload upload = new Upload(HttpConfig.builder().build(), URI.create("http://localhost"), batch -> {}) {
            @Override
            public void retry(Path file, Consumer<Path> noRetry, Consumer<Path> retry) {
                uploadCalled.set(true);
            }
        };
        retryUpload = createRetryUpload(
                RetryConfig.builder()
                        .initialDelaySeconds(0)
                        .delaySeconds(1)
                        .retryAt(List.of(Duration.ofMillis(10)))
                        .build(),
                upload);

        Thread.sleep(50);
        assertThat(uploadCalled.get()).isFalse();
    }
}
