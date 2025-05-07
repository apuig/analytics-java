package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.TrackMessage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        final StorageConfig config =
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

    private RetryUpload createRetryUpload(final RetryConfig retryConfig, final Upload upload) {
        return new RetryUpload(Thread::new, retryConfig, storage, upload);
    }

    private Batch createBatch() {
        final Batch b = new Batch();
        b.setSentAt(Instant.now());
        b.setWriteKey("wk");
        b.setBatch(List.of(new TrackMessage("u", "e")));
        return b;
    }

    @Test
    public void retryDeletesFileOnSuccess() throws Exception {
        storage.write(createBatch());

        final Upload upload =
                new Upload(HttpConfig.builder().gzip(false).build(), URI.create("http://localhost"), batch -> {}) {
                    @Override
                    public void retry(
                            final Path file, final BatchConsumer<Path> noRetry, final BatchConsumer<Path> retry) {
                        uploadCalled.set(true);
                        noRetry.safeProcess(file);
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
        assertThat(tmpFolder.toFile()).isEmptyDirectory();
    }

    @Test
    public void retryCallsHandleRetryOnFailure() throws Exception {
        storage.write(createBatch());

        final Upload upload =
                new Upload(HttpConfig.builder().gzip(false).build(), URI.create("http://localhost"), batch -> {}) {
                    @Override
                    public void retry(
                            final Path file, final BatchConsumer<Path> noRetry, final BatchConsumer<Path> retry) {
                        uploadCalled.set(true);
                        retry.safeProcess(file);
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
    public void runWithNoFilesDoesNothing() throws Exception {
        final Upload upload =
                new Upload(HttpConfig.builder().gzip(false).build(), URI.create("http://localhost"), batch -> {}) {
                    @Override
                    public void retry(
                            final Path file, final BatchConsumer<Path> noRetry, final BatchConsumer<Path> retry) {
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
