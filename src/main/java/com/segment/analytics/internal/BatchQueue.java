package com.segment.analytics.internal;

import com.segment.analytics.config.BatchQueueConfig;
import com.segment.analytics.config.Constants;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.IdentifyMessage;
import com.segment.analytics.dto.Message;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BatchQueue implements Runnable, Closeable {
    private static final Logger LOGGER = Logger.getLogger(BatchQueue.class.getName());
    private final BlockingQueue<MessageWithSize> queue;
    private final Thread pollQueue;
    private final String writeKey;
    private final Map<String, ?> context;
    private final BatchQueueConfig config;
    private final Consumer<Batch> batchConsumer;

    public BatchQueue(
            ThreadFactory threadFactory,
            String writeKey,
            Map<String, ?> context,
            BatchQueueConfig config,
            Consumer<Batch> batchConsumer) {
        this(threadFactory, writeKey, context, config, batchConsumer, true);
    }

    protected BatchQueue(
            ThreadFactory threadFactory,
            String writeKey,
            Map<String, ?> context,
            BatchQueueConfig config,
            Consumer<Batch> batchConsumer,
            boolean startThread) {
        this.writeKey = writeKey;
        this.context = context;
        this.config = config;
        this.batchConsumer = batchConsumer;
        this.queue = new LinkedBlockingQueue<>(config.size);
        this.pollQueue = threadFactory.newThread(this);
        this.pollQueue.setName("segment-" + batchConsumer.getClass().getSimpleName());
        if (startThread) {
            this.pollQueue.start();
        }
    }

    @Override
    public void close() {
        if (pollQueue.isAlive()) {
            pollQueue.interrupt();
            try {
                pollQueue.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * This operation block the calling thread when the queue is full
     */
    public void put(MessageWithSize msg) {
        try {
            queue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean offer(MessageWithSize msg) {
        return queue.offer(msg);
    }

    protected void put(Message msg) {
        put(new MessageWithSize(msg, JSON.sizeInBytes(msg)));
    }

    protected boolean offer(Message msg) {
        return offer(new MessageWithSize(msg, JSON.sizeInBytes(msg)));
    }

    public List<MessageWithSize> drainQueue() {
        List<MessageWithSize> waitingBatch = new ArrayList<>();
        queue.drainTo(waitingBatch);
        return waitingBatch;
    }

    @Override
    public void run() {
        Batch batch = new Batch();
        batch.setWriteKey(writeKey);
        batch.setContext(context);
        // dummy message to avoid empty inclusion
        batch.setBatch(Collections.singletonList(new IdentifyMessage()));
        int batchBaseSize = JSON.sizeInBytes(batch) - JSON.sizeInBytes(new IdentifyMessage());
        batch.setBatch(null);

        List<MessageWithSize> messages = new ArrayList<>(config.flushSize);
        int batchSize = batchBaseSize;
        long firstMessageTime = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            boolean sizeOverflow = false;
            try {
                MessageWithSize message = queue.poll(config.flushMs, TimeUnit.MILLISECONDS);
                if (message != null) {
                    // messages.size() = number of additional commas
                    if (batchSize + message.size + messages.size() <= Constants.MAX_BATCH_SIZE) {
                        if (messages.isEmpty()) {
                            firstMessageTime = message.message.getTimestamp().toEpochMilli();
                        }
                        messages.add(message);
                        batchSize += message.size;
                    } else {
                        sizeOverflow = true;
                    }
                }
                if (messages.isEmpty()) {
                    continue;
                }

                boolean timeout = System.currentTimeMillis() - firstMessageTime > config.flushMs;
                if (timeout || sizeOverflow || message == null || messages.size() >= config.flushSize) {
                    batch = new Batch();
                    batch.setContext(context);
                    batch.setWriteKey(writeKey);
                    batch.setBatch(messages.stream().map(mws -> mws.message).collect(Collectors.toList()));

                    consume(batch);

                    batchSize = batchBaseSize;
                    messages.clear();
                    if (sizeOverflow) {
                        messages.add(message);
                        batchSize += message.size;
                        firstMessageTime = message.message.getTimestamp().toEpochMilli();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        queue.addAll(messages);
    }

    private void consume(Batch batch) {
        try {
            batchConsumer.accept(batch);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e, () -> "Unexpected error processing batch. %d events will be lost"
                    .formatted(batch.getBatch().size()));
        }
    }
}
