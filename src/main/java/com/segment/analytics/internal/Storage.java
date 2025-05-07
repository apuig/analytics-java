package com.segment.analytics.internal;

import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.Batch;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Write a {@link Batch} into a file of the configured folder.
 * <p>
 * File convention to encode {@code creationTime}_{@code retryAfterTime}_{@code retryAttempt}_{@code UUID}
 * </p>
 * All the operations use write to tmp, then atomic rename to a new, non-existing filename.
 *  */
public class Storage implements BatchConsumer<Batch>, Closeable {
    private static final Logger LOGGER = Logger.getLogger(Storage.class.getName());

    private static final String TMP_EXTENSION = ".tmp";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+)_(\\d+)_(\\d+)_([a-fA-F0-9\\-]+)$");

    private static String formatFileName(
            final long creationTime, final long retyAfter, final long retry, final String uuid) {
        return String.format("%d_%d_%d_%s", creationTime, retyAfter, retry, uuid);
    }

    private final Path folder;
    private final List<Duration> retryAt;

    /**
     * @throws IOException if cannot create the configured filePath directory
     */
    public Storage(final RetryConfig retryConfig, final StorageConfig config) throws IOException {
        this.retryAt = retryConfig.retryAt;
        this.folder = Files.createDirectories(Path.of(config.filePath));
        if (!folder.toFile().canWrite()) {
            throw new IOException("Expecting write access in " + folder);
        }
    }

    public void write(final Batch batch) {
        final long time = batch.getSentAt().toEpochMilli();
        final String fileName = formatFileName(
                time, time + retryAt.get(0).toMillis(), 0, UUID.randomUUID().toString());
        final Path path = folder.resolve(fileName + TMP_EXTENSION);

        try (FileChannel fileChannel = FileChannel.open(
                        path, StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE_NEW);
                Writer w = Channels.newWriter(fileChannel, StandardCharsets.UTF_8)) {

            JSON.write(batch, w);

            tryMoveToSibling(path, fileName);
        } catch (final IOException e) {
            LOGGER.log(Level.WARNING, e, () -> "Cannot write batch file " + path);
        }
    }

    public Queue<FileEntry> listFilesWithRetryAfterNow(final int max) {
        int count = 0;
        final PriorityQueue<FileEntry> queue = new PriorityQueue<>();
        final long now = System.currentTimeMillis();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(folder)) {
            for (final Path file : files) {
                final long retryAfter = retryAfter(file);
                if ((retryAfter != -1) && (now >= retryAfter)) {
                    queue.offer(new FileEntry(file, retryAfter));
                    count++;
                    if (count >= max) {
                        break;
                    }
                }
            }
        } catch (final IOException e) {
            LOGGER.log(Level.WARNING, e, () -> "Cannot list directory " + folder);
        }
        return queue;
    }

    public void tryDelete(final Path file) {
        try {
            Files.delete(file);
        } catch (final IOException e) {
            LOGGER.log(Level.WARNING, e, () -> "Failed to delete " + file);
        }
    }

    public Path tryMoveToTmp(final Path file) {
        return tryMoveToSibling(file, file.getFileName() + TMP_EXTENSION);
    }

    /**
     * Move temporal file updating {@code retryAfterTime} and {@code retryAttempt}. File is deleted if was the last retry.
     */
    public void handleRetry(final Path tmpFile) {
        final Path pathName = tmpFile.getFileName();
        if (pathName == null) {
            return;
        }
        String fileName = pathName.toString();
        if (!fileName.endsWith(TMP_EXTENSION)) {
            throw new IllegalArgumentException(
                    "Expecting extension '%s' in fileName '%s'".formatted(TMP_EXTENSION, fileName));
        }
        fileName = fileName.substring(0, fileName.length() - TMP_EXTENSION.length());

        final Matcher matcher = FILENAME_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            throw new IllegalStateException("unexpected fileName " + fileName);
        }

        final long createdAt = Long.parseLong(matcher.group(1));
        final int nextRetry = Integer.parseInt(matcher.group(3)) + 1;
        final String uuid = matcher.group(4);

        if (nextRetry >= retryAt.size()) {
            tryDelete(tmpFile);
            LOGGER.log(Level.WARNING, () -> "Expired file " + tmpFile);
            return;
        }

        final long retryAfter = createdAt + retryAt.get(nextRetry).toMillis();
        final String newName = formatFileName(createdAt, retryAfter, nextRetry, uuid);

        if (tryMoveToSibling(tmpFile, newName) == null) {
            LOGGER.log(Level.WARNING, () -> "Failed to reschedule " + tmpFile);
        }
    }

    private static Path tryMoveToSibling(final Path file, final String newFileName) {
        final Path newFile = file.resolveSibling(newFileName);
        try {
            return Files.move(file, newFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (final IOException e) {
            LOGGER.log(Level.FINER, e, () -> "Cannot move batch file " + file + " , keeping it");
            return null;
        }
    }

    private static long retryAfter(final String fileName) {
        final Matcher matcher = FILENAME_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return -1;
        }
        return Long.parseLong(matcher.group(2));
    }

    private static long retryAfter(final Path path) {
        final Path namePath = path.getFileName();
        if (namePath == null) {
            return -1;
        }
        final String fileName = namePath.toString();
        if (!Files.isRegularFile(path) || fileName.endsWith(TMP_EXTENSION)) {
            return -1;
        }
        return retryAfter(fileName);
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void process(final Batch batch) throws Exception {
        write(batch);
    }
}
