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
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
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
                    lock.lock();
                    try {
                        msgs = read();
                        if (msgs.isEmpty()) {
                            continue;
                        }
                        // FIXME now its reading all the msgs and waits until all is processed
                        // it will be better to work with batch and truncate the file
                        file.delete();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        lastMessage = System.currentTimeMillis();
                        continue;
                    } finally {
                        lock.unlock();
                    }

                    // FIXME batch
                    while (!msgs.isEmpty()) {
                        boolean canEnqueue = true;
                        for (int i = msgs.size() - 1; canEnqueue && i >= 0; i--) {
                            Message msg = msgs.get(i);
                            canEnqueue = client.offer(msg);
                            if (canEnqueue) {
                                msgs.remove(i);
                                System.err.println("reenqueued " + msg.messageId());
                            }
                        }
                        try {
                            Thread.sleep(1_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    lastMessage = System.currentTimeMillis();
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

    List<Message> read() throws IOException {
        if (file.exists()) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                fileChannel.lock(0, Long.MAX_VALUE, true);

                final String[] lines = new String(
                                Channels.newInputStream(fileChannel).readAllBytes(), StandardCharsets.UTF_8)
                        .split(System.lineSeparator());
                return Arrays.stream(lines)
                        .map(m -> fromJson(m))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            }
        } else {
            return Collections.emptyList();
        }
    }

    private void write(List<Message> batch) {
        lock.lock();
        try (FileChannel fileChannel = FileChannel.open(
                file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
            fileChannel.lock();

            final String lines = batch.stream()
                    .map(this::toJson)
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(System.lineSeparator()));

            OutputStream os = Channels.newOutputStream(fileChannel);
            os.write(lines.getBytes(StandardCharsets.UTF_8));
            os.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
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
