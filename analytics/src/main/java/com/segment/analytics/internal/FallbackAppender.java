package com.segment.analytics.internal;

import com.google.gson.Gson;
import com.segment.analytics.internal.Config.FileConfig;
import com.segment.analytics.messages.Batch;
import com.segment.analytics.messages.Message;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

  private Path currentFile;
  private Instant currentStart;
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

    rollover();

    this.queue = new ArrayBlockingQueue<Message>(config.size);
    this.writer = threadFactory.newThread(new FileWriter());
    this.writer.setName(FallbackAppender.class.getSimpleName() + "-Writer");
    this.writer.start();
  }

  /** Ends the currentFile and start a new one */
  private void rollover() {
    String fileName;
    if (currentFile != null) {
      fileName = currentFile.getFileName().toString();
      try {
	Files.move(currentFile,
	    currentFile.resolveSibling(fileName.substring(0, fileName.length() - TMP_EXTENSION.length())),
	    StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException e) {
	LOGGER.log(Level.WARNING, "Cannot rollover " + fileName, e);
      }
    }

    currentStart = Instant.now();
    fileName = String.format("%s-%s%s", currentStart.toEpochMilli(), UUID.randomUUID(), TMP_EXTENSION);
    this.currentFile = directory.resolve(fileName);
  }

  @Override
  public void close() {
    writer.interrupt();
  }

  /** Write a new file with the content of a batch */
  public void add(Batch batch) {
    String fileName = String.format("%s-%s", batch.sentAt().getTime(), UUID.randomUUID());
    Path path = directory.resolve(fileName + TMP_EXTENSION);

    try (
	FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
	    StandardOpenOption.CREATE_NEW);
	OutputStream os = Channels.newOutputStream(fileChannel)) {

      os.write(toJson(batch).getBytes(StandardCharsets.UTF_8));

      fileChannel.force(true);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Cannot write file batch file " + fileName, e);
    }

    try {
      Files.move(path, path.resolveSibling(fileName), StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Cannot move file batch file " + fileName, e);
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
      LOGGER.log(Level.FINEST, "adding to fallback " + msg.messageId());
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

	  if (Duration.between(currentStart, Instant.now()).getSeconds() > 60 && currentFile.toFile().exists()) {
	    endCurrentFile();
	    rollover();
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
  private static final byte[] BATCH_END = "],\"sentAt\":\"2023-04-19T04:03:46.880Z\",\"writeKey\":\"mywrite\"}"
      .getBytes(StandardCharsets.UTF_8);
  // FIXME DateTimeUtils
  // FIXME mywrite

  private void write(List<Message> batch) {
    List<Message> remaining = writeInternal(batch);
    if (!remaining.isEmpty()) {
      rollover();
      writeInternal(remaining);
    }

    batch.clear();
  }

  /** @return messages that do not fit in the current file */
  private List<Message> writeInternal(List<Message> batch) {
    try (
	FileChannel fileChannel = FileChannel.open(currentFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
	    StandardOpenOption.CREATE);
	OutputStream os = Channels.newOutputStream(fileChannel);
	FileLock fileLock = fileChannel.lock();) {

      long currentFileSize = fileChannel.size();
      boolean first = currentFileSize == 0;
      if (first) {
	os.write(BATCH_BEGIN);
      }

      for (int i = 0; i < batch.size(); i++) {
	Message msg = batch.get(i);
	byte[] msgBytes = toJson(msg).getBytes(StandardCharsets.UTF_8);
	if (msgBytes.length + currentFileSize + COMMA.length + BATCH_END.length > MAX_BATCH_SIZE) {
	  os.write(BATCH_END);
	  fileChannel.force(true);

	  return batch.subList(i, batch.size());
	}

	if (first) {
	  first = false;
	} else {
	  os.write(COMMA);
	}
	os.write(msgBytes);
      }

      fileChannel.force(true);

      return Collections.emptyList();
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "write file " + currentFile, e);
      return Collections.emptyList();
    }
  }

  private void endCurrentFile() {
    try (
	FileChannel fileChannel = FileChannel.open(currentFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
	    StandardOpenOption.CREATE);
	OutputStream os = Channels.newOutputStream(fileChannel);
	FileLock fileLock = fileChannel.lock();) {
      os.write(BATCH_END);
      fileChannel.force(true);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "write file " + currentFile, e);
    }
  }

  private String toJson(final Object msg) {
    try {
      return gson.toJson(msg);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
