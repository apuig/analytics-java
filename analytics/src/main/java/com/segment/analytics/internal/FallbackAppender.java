package com.segment.analytics.internal;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.segment.analytics.gson.AutoValueAdapterFactory;
import com.segment.analytics.gson.ISO8601DateAdapter;
import com.segment.analytics.messages.Message;
import com.segment.analytics.messages.TrackMessage;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.io.FileSystem;

public class FallbackAppender {

    private static final int FLUSH_MS = 100;
    private static final int BATCH = 20;
    private static final int LASTMESSAGE_RETRY_MS = 10_000;
    private static final String PATH = "pending";

    private final AnalyticsClient client;
    private final BlockingQueue<Message> queue;
    private final File file;
    private final Lock lock = new ReentrantLock();
    private final Thread writer;
    private final Thread reader;
    private final Gson gson;

    private transient long lastMessage;

    public FallbackAppender(AnalyticsClient client) {
        this.client = client;
        this.file = new File(PATH);
        this.queue = new ArrayBlockingQueue<Message>(100);
        this.writer = new Thread(new FileWriter()); // XXX threadFactory daemon
        this.reader = new Thread(new FileReader()); // XXX threadFactory daemon
        this.gson = new GsonBuilder()
                .registerTypeAdapterFactory(new AutoValueAdapterFactory())
                .registerTypeAdapter(Date.class, new ISO8601DateAdapter())
                .create();

        file.delete(); // FIXME do not remove on start

        this.lastMessage = System.currentTimeMillis();
        this.writer.start();
        this.reader.start();
    }

    public void close() {
        reader.interrupt();
        writer.interrupt();
    }

    // block !!!
    public void add(Message msg) {
        try {
            System.err.println("failed " + msg.messageId());
            queue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    class FileReader implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                if (queue.isEmpty() && System.currentTimeMillis() - lastMessage > LASTMESSAGE_RETRY_MS) {
                    if (file.length() == 0) {
                        continue;
                    }

                    List<Message> msgs;
                    try {
                        msgs = truncate(20); // XXX messageSize
                        if (msgs.isEmpty()) {
                            continue;
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        lastMessage = System.currentTimeMillis();
                        continue;
                    }

                    while (!msgs.isEmpty()) {
                        boolean canEnqueue = true;
                        for (int i = msgs.size() - 1; canEnqueue && i >= 0; i--) {
                            Message msg = msgs.get(i);
                            canEnqueue = client.offer(msg);
                            if (canEnqueue) {
                                msgs.remove(i);
                                System.err.println("reenqueued " + msg.messageId());
                            } else {
                                // slow down next iteration when http queue overflow
                                try {
                                    Thread.sleep(1_000);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    }
                }

                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    class FileWriter implements Runnable {
        @Override
        public void run() {
            final List<Message> batch = new ArrayList<>();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final Message msg = queue.poll(FLUSH_MS, TimeUnit.MILLISECONDS);
                    if (msg == null) {
                        if (!batch.isEmpty()) {
                            write(batch);
                        }
                    } else {
                        batch.add(msg);
                        if (batch.size() >= BATCH) {
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

    List<Message> truncate(int numMessages) throws IOException {
        lock.lock();

        if (!file.exists()) {
            lock.unlock();
            return Collections.emptyList();
        }

        try (ReversedLinesFileReader reader = ReversedLinesFileReader.builder()
                .setPath(file.toPath())
                .setBufferSize(FileSystem.getCurrent().getBlockSize())
                .setCharset(StandardCharsets.UTF_8)
                .get()) {

            return reader.readLines(numMessages).stream()
                    .map(this::fromJson)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    private static final byte[] NEW_LINE = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    private void write(List<Message> batch) {
        lock.lock();
        try (FileChannel fileChannel = FileChannel.open(
                        file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                OutputStream os = Channels.newOutputStream(fileChannel);
                FileLock fileLock = fileChannel.lock(); ) {

            for (Message msg : batch) {
                os.write(toJson(msg).getBytes(StandardCharsets.UTF_8));
                os.write(NEW_LINE);
            }

            fileChannel.force(true);

            batch.clear();

            lastMessage = System.currentTimeMillis();
        } catch (IOException e) {
            e.printStackTrace(); // FIXME
        } finally {
            lock.unlock();
        }
    }

    private String toJson(final Message msg) {
        try {
            return gson.toJson(msg);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private Message fromJson(final String msg) {
        try {
            // FIXME only track
            return gson.fromJson(msg, TrackMessage.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
