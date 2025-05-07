package com.segment.analytics.internal;

import com.segment.analytics.config.BatchQueueConfig;
import com.segment.analytics.config.Constants;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.Message;
import com.segment.analytics.dto.TrackMessage;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class BatchQueue implements Runnable, Closeable {
    private final BlockingQueue<MessageWithSize> queue;
    private final Thread pollQueue;
    private final String writeKey;
    private final Map<String, ?> context;
    private final BatchQueueConfig config;
    private final BatchConsumer<Batch> batchConsumer;

    public BatchQueue(
            final ThreadFactory threadFactory,
            final String writeKey,
            final Map<String, ?> context,
            final BatchQueueConfig config,
            final BatchConsumer<Batch> batchConsumer) {
        this(threadFactory, writeKey, context, config, batchConsumer, true);
    }

    protected BatchQueue(
            final ThreadFactory threadFactory,
            final String writeKey,
            final Map<String, ?> context,
            final BatchQueueConfig config,
            final BatchConsumer<Batch> batchConsumer,
            final boolean startThread) {
        this.writeKey = writeKey;
        this.context = context;
        this.config = config;
        this.batchConsumer = batchConsumer;
        this.queue = new ArrayBlockingQueue<>(config.size);
        this.pollQueue = threadFactory.newThread(this);
        this.pollQueue.setName("segment-" + batchConsumer.getClass().getSimpleName());
        if (startThread) {
            this.pollQueue.start();
        }
    }

    @Override
    public void close() {
        if (pollQueue.isAlive()) {
            try {
                // when blocking configured attempt to consume the queue
                if ((config.blockTimeout != 0) && !queue.isEmpty()) {
                    Thread.sleep(config.blockTimeout);
                }
                pollQueue.interrupt();
                pollQueue.join();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * This operation block the calling thread for a configured time when the queue is full
     * @return False when the thread was interrupted or timeout
     */
    public boolean put(final MessageWithSize msg) {
        try {
            return config.blockTimeout == 0
                    ? queue.offer(msg)
                    : queue.offer(msg, config.blockTimeout, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected boolean put(final Message msg) {
        return put(new MessageWithSize(msg, JSON.sizeInBytes(msg)));
    }

    public List<MessageWithSize> drainQueue() {
        final List<MessageWithSize> waitingBatch = new ArrayList<>();
        queue.drainTo(waitingBatch);
        return waitingBatch;
    }

    @Override
    public void run() {
        Batch batch = new Batch();
        batch.setWriteKey(writeKey);
        batch.setContext(context);
        // dummy message to avoid empty inclusion
        final Message dummy = new TrackMessage("u", "e");
        batch.setBatch(Collections.singletonList(dummy));
        final int batchBaseSize = JSON.sizeInBytes(batch) - JSON.sizeInBytes(dummy);
        batch.setBatch(null);

        final List<MessageWithSize> messages = new ArrayList<>(config.flushSize);
        int batchSize = batchBaseSize;
        long firstMessageTime = System.currentTimeMillis();

        while (!Thread.currentThread().isInterrupted()) {
            boolean sizeOverflow = false;
            try {
                final MessageWithSize message = queue.poll(config.flushMs, TimeUnit.MILLISECONDS);
                if (message != null) {
                    // messages.size() = number of additional commas
                    if ((batchSize + message.getSize() + messages.size()) <= Constants.MAX_BATCH_SIZE) {
                        if (messages.isEmpty()) {
                            firstMessageTime =
                                    message.getMessage().getTimestamp().toEpochMilli();
                        }
                        messages.add(message);
                        batchSize += message.getSize();
                    } else {
                        sizeOverflow = true;
                    }
                }
                if (messages.isEmpty()) {
                    continue;
                }

                final boolean timeout = (System.currentTimeMillis() - firstMessageTime) > config.flushMs;
                if (timeout || sizeOverflow || (message == null) || (messages.size() >= config.flushSize)) {
                    batch = new Batch();
                    batch.setContext(context);
                    batch.setWriteKey(writeKey);
                    batch.setBatch(
                            messages.stream().map(MessageWithSize::getMessage).toList());

                    batchConsumer.safeProcess(batch);

                    batchSize = batchBaseSize;
                    messages.clear();
                    if (sizeOverflow) {
                        messages.add(message);
                        batchSize += message.size;
                        firstMessageTime = message.getMessage().getTimestamp().toEpochMilli();
                    }
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        queue.addAll(messages);
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }
}
