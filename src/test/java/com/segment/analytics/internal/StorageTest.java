package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Queue;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.TrackMessage;

public class StorageTest {

    Path tmpFolder;
    StorageConfig config;
    RetryConfig retry;

    @Before
    public void setup() throws IOException {
        tmpFolder = Files.createTempDirectory("storagetest");
        config = StorageConfig.builder().filePath(tmpFolder.toString()).build();
        retry = RetryConfig.builder().retryAt(List.of(Duration.ofMillis(100))).build();
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
        // Then the create fileName contains information to respect the initial retryAfter
        String fileName = assertThat(tmpFolder.toFile().listFiles())
                .singleElement()
                .actual()
                .getName();
        assertThat(fileName)
                .startsWith(String.valueOf(now.toEpochMilli()))
                .contains("_%d_".formatted(now.toEpochMilli() + initialDelay.toMillis()))
                .contains("_0_");
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
        assertThat(path.toFile()).doesNotExist();
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
        Storage s = new Storage(retry, config);
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
        assertThat(path.toFile()).doesNotExist();
        // And no more files are created
        assertThat(tmpFolder.toFile()).isEmptyDirectory();
    }

    @Test
    public void writeAndReadFileContent() throws IOException {
        Storage s = new Storage(retry, config);
        // Given a batch
        Batch b = new Batch();
        b.setSentAt(Instant.now());
        b.setWriteKey("wk");
        b.setBatch(List.of(new TrackMessage()));
        // When its written
        s.write(b);
        // Then the file can be listed
        File[] files = tmpFolder.toFile().listFiles();
        assertThat(files).hasSize(1);
        String fileName = files[0].getName();
        // And the file contains the write key and the batch element
        String content = Files.readString(tmpFolder.resolve(fileName), StandardCharsets.UTF_8);
        assertThat(content).contains("\"wk\"").contains("\"batch\"").contains("\"type\":\"track\"");
    }

    @Test
    public void tryDeleteRemovesFile() throws IOException {
        Storage s = new Storage(retry, config);
        // Given a file
        Path file = tmpFolder.resolve("todelete.txt");
        Files.writeString(file, "delete me", StandardOpenOption.CREATE_NEW);
        assertThat(file.toFile()).exists();
        // When try delete
        s.tryDelete(file);
        // Then the file is deleted
        assertThat(file.toFile()).doesNotExist();
    }

    @Test
    public void tryDeleteDoNotThrow() throws IOException {
        Storage s = new Storage(retry, config);
        // Given a unexisting file
        Path file = tmpFolder.resolve("donotexist.txt");
        assertThat(file.toFile()).doesNotExist();
        // When try delete
        s.tryDelete(file);
        // Then the method do not throw
    }

    @Test
    public void tryMoveToTmpRenamesFile() throws IOException {
        Storage s = new Storage(retry, config);
        // Given a file
        Path file = tmpFolder.resolve("movefile.txt");
        Files.writeString(file, "move me", StandardOpenOption.CREATE_NEW);
        // When try move to tmp
        Path tmpFile = s.tryMoveToTmp(file);
        // Then the extension is added
        assertThat(tmpFile).isNotNull();
        assertThat(tmpFile.getFileName().toString()).endsWith(".tmp");
        assertThat(tmpFile.toFile()).exists();
        assertThat(file.toFile()).doesNotExist();
    }

    @Test
    public void handleRetryWithInvalidFileNameThrows() throws IOException {
        Storage s = new Storage(retry, config);
        // Given a file with invalid name
        Path invalidFile = tmpFolder.resolve("invalidfilename.tmp");
        Files.writeString(invalidFile, "{}", StandardOpenOption.CREATE_NEW);

        // When handleTretry
        try {
            s.handleRetry(invalidFile);
        } catch (IllegalStateException e) {
            // Then it fails
            assertThat(e.getMessage()).contains("unexpected fileName");
        }
    }

    @Test
    public void handleRetryWithNonTmpFileThrows() throws IOException {
        Storage s = new Storage(retry, config);
        // Given a file with invalid extension
        Path nonTmpFile = tmpFolder.resolve("notmpfile.txt");
        Files.writeString(nonTmpFile, "{}", StandardOpenOption.CREATE_NEW);
        // When handleTretry
        try {
            s.handleRetry(nonTmpFile);
        } catch (IllegalArgumentException e) {
            // Then it fails
            assertThat(e.getMessage()).contains("Expecting extension");
        }
    }

    @Test
    public void cannotWriteToFolderThrows() throws IOException {
        // Given a read only folder
        Path readOnlyFolder = Files.createTempDirectory("readonly");
        readOnlyFolder.toFile().setWritable(false);
        StorageConfig readOnlyConfig =
                StorageConfig.builder().filePath(readOnlyFolder.toString()).build();
        // When instantiation storage
        try {
            new Storage(retry, readOnlyConfig);
        } catch (IOException e) {
            // Then it fails
            assertThat(e.getMessage()).contains("Expecting write access");
        } finally {
            readOnlyFolder.toFile().setWritable(true);
            readOnlyFolder.toFile().delete();
        }
    }

    @Test
    public void listFilesWithMaxLimit() throws Exception {
        // Given a retry config to check the next 1 ms
        Storage s = new Storage(
                RetryConfig.builder().retryAt(List.of(Duration.ofMillis(1))).build(), config);
        // And 5 batches
        Instant now = Instant.now();
        for (int i = 0; i < 5; i++) {
            Batch b = new Batch();
            b.setSentAt(now);
            b.setWriteKey("wk");
            b.setBatch(List.of(new TrackMessage()));
            s.write(b);
        }
        // When await (so all files are eligible to retry)
        Awaitility.await().atLeast(Duration.ofMillis(10)).until(() -> !s.listFilesWithRetryAfterNow(1)
                .isEmpty());
        // Then only 3 results are returned
        Queue<FileEntry> files = s.listFilesWithRetryAfterNow(3);
        assertThat(files).hasSize(3);
    }
}
