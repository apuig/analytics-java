package com.segment.analytics.internal;

import com.google.gson.Gson;
import com.segment.analytics.internal.Config.FileConfig;
import com.segment.analytics.messages.Batch;
import com.segment.analytics.messages.Message;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FallbackAppender implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(FallbackAppender.class.getName());

    public static final String TMP_EXTENSION = ".tmp";

    /**
     * Our servers only accept batches < 500KB. This limit is 475KB to account for
     * extra data that is not present in payloads themselves, but is added later,
     * such as `sentAt`, `integrations` and other json tokens.
     */
    private static final long MAX_BATCH_SIZE = 475_000; // 475KB.

    private Path currentFileOverflow;
    private Instant currentStartOverflow;
    private Path currentFileBatch;
    private Instant currentStartBatch;
    private Lock currentFileBatchLock = new ReentrantLock();
    private long currentLineSize = 0;
    boolean firstEventInBatch = true;

    private final Path directory;
    private final Gson gson;
    private final FileConfig config;
    private final BlockingQueue<Message> queue;
    private final Thread writer;

    /**
     * @throws IOException if cannot create the configured filePath directory
     */
    public FallbackAppender(Gson gson, ThreadFactory threadFactory, FileConfig config) throws IOException {
        this.gson = gson;
        this.config = config;
        this.directory = Files.createDirectories(Path.of(config.filePath));

        rolloverOverflow();
        rolloverBatch();

        this.queue = new ArrayBlockingQueue<Message>(config.size);
        this.writer = threadFactory.newThread(new FileWriter());
        this.writer.setName(FallbackAppender.class.getSimpleName());
        this.writer.start();
    }

    /** Ends the currentFile and start a new one */
    private void rolloverOverflow() {
        String fileName;
        if (currentFileOverflow != null) {
            fileName = currentFileOverflow.getFileName().toString();
            try {
                Files.move(
                        currentFileOverflow,
                        currentFileOverflow.resolveSibling(
                                fileName.substring(0, fileName.length() - TMP_EXTENSION.length())),
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Cannot rollover " + fileName, e);
            }
        }

        currentStartOverflow = Instant.now();
        fileName = String.format("%s-%s%s", currentStartOverflow.toEpochMilli(), UUID.randomUUID(), TMP_EXTENSION);
        this.currentFileOverflow = directory.resolve(fileName);
        LOGGER.log(Level.FINE, "currentFileOverflow : {0}", fileName);
        firstEventInBatch = true;
    }

    private void rolloverBatch() {
        currentFileBatchLock.lock();
        try {

            String fileName;
            if (currentFileBatch != null) {
                fileName = currentFileBatch.getFileName().toString();
                try {
                    Files.move(
                            currentFileBatch,
                            currentFileBatch.resolveSibling(
                                    fileName.substring(0, fileName.length() - TMP_EXTENSION.length())),
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Cannot rollover " + fileName, e);
                }
            }

            currentStartBatch = Instant.now();
            fileName = String.format("%s-%s%s", currentStartBatch.toEpochMilli(), UUID.randomUUID(), TMP_EXTENSION);
            this.currentFileBatch = directory.resolve(fileName);
            LOGGER.log(Level.FINE, "currentFileBatch : {0}", fileName);
        } finally {
            currentFileBatchLock.unlock();
        }
    }

    @Override
    public void close() {
        writer.interrupt();
    }

    /** Write a new file with the content of a batch */
    public void add(Batch batch) {
        currentFileBatchLock.lock();
        try (FileChannel fileChannel = FileChannel.open(
                        currentFileBatch,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE);
                Writer w = Channels.newWriter(fileChannel, StandardCharsets.UTF_8)) {

            saveBatch(batch, w);
            if (fileChannel.size() > config.rolloverMaxSizeBytes) {
                rolloverBatch();
            } else {
                w.write(System.lineSeparator());
            }
            // TODO fileChannel.force(true);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Cannot write file batch file " + currentFileBatch, e);
        } finally {
            currentFileBatchLock.unlock();
        }
    }

    /**
     * Add elements to be persisted. Called on httpQueue overflow
     * <p>
     * This operation may block the calling thread
     * </p>
     */
    public void add(Message msg) {
        try {
            LOGGER.log(Level.FINEST, "adding to fallback {0}", msg.messageId());
            queue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    class FileWriter implements Runnable {
        @Override
        public void run() {
            final List<Message> batch = new ArrayList<>(config.flushSize);
            while (!Thread.currentThread().isInterrupted()) {
                try {

                    if ((Duration.between(currentStartOverflow, Instant.now()).getSeconds()
                                            > config.rolloverTimeoutSeconds
                                    && currentFileOverflow.toFile().exists())
                            || currentFileOverflow.toFile().length() > config.rolloverMaxSizeBytes) {
                        endCurrentLine();
                        rolloverOverflow();
                    }

                    if (Duration.between(currentStartBatch, Instant.now()).getSeconds() > config.rolloverTimeoutSeconds
                            && currentFileBatch.toFile().exists()) {
                        rolloverBatch();
                    }

                    final Message msg = queue.poll(config.flushMs, TimeUnit.MILLISECONDS);
                    if (msg == null) {
                        if (!batch.isEmpty()) {
                            write(batch);
                        }
                    } else {
                        batch.add(msg);
                        if (batch.size() >= config.flushSize) {
                            write(batch);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!batch.isEmpty()) {
                write(batch);
            }
        }
    }

    private static final byte[] BATCH_BEGIN = "{\"batch\":[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMA = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW_LINE = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
    private static final byte[] BATCH_END =
            "],\"sentAt\":\"2023-04-19T04:03:46.880Z\",\"writeKey\":\"mywrite\"}".getBytes(StandardCharsets.UTF_8);
    // FIXME DateTimeUtils
    // FIXME mywrite

    private void write(List<Message> batch) {
        writeInternal(batch);
        batch.clear();
    }

    private void writeInternal(List<Message> batch) {
        try (FileChannel fileChannel = FileChannel.open(
                        currentFileOverflow,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE);
                OutputStream os = Channels.newOutputStream(fileChannel)) {

            if (firstEventInBatch) {
                os.write(BATCH_BEGIN);
                currentLineSize = BATCH_BEGIN.length;
            }

            for (int i = 0; i < batch.size(); i++) {
                Message msg = batch.get(i);
                byte[] msgBytes = toJson(msg).getBytes(StandardCharsets.UTF_8);
                if (msgBytes.length + currentLineSize + COMMA.length + BATCH_END.length > MAX_BATCH_SIZE) {
                    os.write(BATCH_END);
                    os.write(NEW_LINE);
                    os.write(BATCH_BEGIN);
                    currentLineSize = BATCH_BEGIN.length;
                    firstEventInBatch = true;
                }

                if (firstEventInBatch) {
                    firstEventInBatch = false;
                } else {
                    os.write(COMMA);
                    currentLineSize += COMMA.length;
                }
                os.write(msgBytes);
                currentLineSize += msgBytes.length;
            }
            // TODO fileChannel.force(true);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "write file " + currentFileOverflow, e);
        }
    }

    private void endCurrentLine() {
        try (FileChannel fileChannel =
                        FileChannel.open(currentFileOverflow, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                OutputStream os = Channels.newOutputStream(fileChannel)) {
            if (currentLineSize == BATCH_BEGIN.length) {
                fileChannel.truncate(fileChannel.size() - (BATCH_BEGIN.length + NEW_LINE.length));
            } else {
                os.write(BATCH_END);
            }
            // TODO fileChannel.force(true);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "write file " + currentFileOverflow, e);
        }
    }

    private String toJson(final Object msg) {
        try {
            return gson.toJson(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void saveBatch(final Batch batch, Writer file) {
        try {
            gson.toJson(batch, file);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
