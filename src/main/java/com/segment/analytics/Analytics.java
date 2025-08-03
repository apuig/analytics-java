package com.segment.analytics;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.segment.analytics.config.Constants;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.config.HttpConfig;
import com.segment.analytics.config.RetryConfig;
import com.segment.analytics.config.StorageConfig;
import com.segment.analytics.dto.Message;
import com.segment.analytics.internal.BatchQueue;
import com.segment.analytics.internal.JSON;
import com.segment.analytics.internal.MessageWithSize;
import com.segment.analytics.internal.RetryUpload;
import com.segment.analytics.internal.Storage;
import com.segment.analytics.internal.Upload;

public class Analytics implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(Analytics.class.getName());

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    private final BatchQueue uploadQueue;
    private final Upload upload;
    private final BatchQueue storageQueue;
    private final Storage storage;
    private final RetryUpload retry;

    Analytics(BatchQueue uploadQueue, Upload upload, BatchQueue storageQueue, Storage storage, RetryUpload retry) {
        this.uploadQueue = uploadQueue;
        this.upload = upload;
        this.storageQueue = storageQueue;
        this.storage = storage;
        this.retry = retry;
    }

    public void enqueue(Message message) throws IllegalArgumentException {
        if (isShutDown.get()) {
            LOGGER.log(Level.WARNING, () -> "Attempt to enqueue a message when shutdown has been called " + message);
            return;
        }
        if (message.getMessageId() == null) {
            throw new IllegalArgumentException("Message without messageId");
        }
        int size = JSON.sizeInBytes(message);
        if (size > Constants.MSG_MAX_SIZE) {
            throw new IllegalArgumentException(
                    "Message was above individual limit. MessageId: " + message.getMessageId());
        }
        MessageWithSize mws = new MessageWithSize(message, size);
        if (!uploadQueue.offer(mws)) {
            // TODO option to use offer also in storageQueue
            storageQueue.put(mws); // possible block when queue is full
            LOGGER.log(Level.FINEST, () -> "overflow " + message.getMessageId());
        } else {
            LOGGER.log(Level.FINEST, () -> "enqueued " + message.getMessageId());
        }
    }

    @Override
    public void close() {
        if (isShutDown.compareAndSet(false, true)) {
            retry.close();
            uploadQueue.close();
            upload.close();

            for (MessageWithSize msg : uploadQueue.drainQueue()) {
                storageQueue.put(msg);
            }

            // TODO add option to enforce storage is always flushed
            storageQueue.close();
            for (MessageWithSize msg : storageQueue.drainQueue()) {
                LOGGER.log(
                        Level.SEVERE,
                        () -> "Lost overflow event: " + new String(JSON.toJson(msg.message), StandardCharsets.UTF_8));
            }
        }
    }

    public static Builder builder(String writeKey) {
        if (writeKey == null || writeKey.isEmpty() || writeKey.length() > 32) {
            throw new IllegalArgumentException("Expecting writeKey lenght < 32");
        }
        return new Builder(writeKey);
    }

    public static class Builder {
        private final String writeKey;
        private URI uri;
        private ThreadFactory threadFactory;
        private HttpConfig httpConfig;
        private StorageConfig storageConfig;
        private RetryConfig retryConfig;
        private Map<String, ?> context;

        Builder(String writeKey) {
            this.writeKey = writeKey;
        }

        public Builder endpoint(String endpoint) throws URISyntaxException {
            if (endpoint == null || endpoint.isBlank()) {
                throw new NullPointerException("endpoint cannot be null or blank.");
            }
            this.uri = new URI(endpoint + Defaults.DEFAULT_PATH);
            return this;
        }

        public Builder httpConfig(HttpConfig httpConfig) {
            this.httpConfig = httpConfig;
            return this;
        }

        public Builder storageConfig(StorageConfig storageConfig) {
            this.storageConfig = storageConfig;
            return this;
        }

        public Builder retryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }

        public Builder context(Map<String, ?> context) {
            this.context = context;
            return this;
        }

        /**
         * Instantiate the analytics client
         * @throws IOException if cannot create the configured filePath directory
         */
        public Analytics build() throws IOException {
            if (uri == null) {
                uri = URI.create(Defaults.DEFAULT_ENDPOINT + Defaults.DEFAULT_PATH);
            }
            if (threadFactory == null) {
                threadFactory = Defaults.defaultThreadFactory();
            }
            if (httpConfig == null) {
                httpConfig = HttpConfig.builder().build();
            }
            if (storageConfig == null) {
                storageConfig = StorageConfig.builder().build();
            }
            if (retryConfig == null) {
                retryConfig = RetryConfig.builder().build();
            }

            Map<String, Object> batchContext = new HashMap<>();
            if (context != null) {
                batchContext.putAll(context);
            }
            batchContext.put("library", Map.of("name", "analytics-java", "version", Constants.VERSION));

            Storage storage = new Storage(retryConfig, storageConfig);
            Upload upload = new Upload(httpConfig, uri, storage::write);

            BatchQueue storageQueue =
                    new BatchQueue(threadFactory, writeKey, batchContext, storageConfig, storage::write);
            BatchQueue uploadQueue = new BatchQueue(threadFactory, writeKey, batchContext, httpConfig, upload::upload);

            RetryUpload retry = new RetryUpload(threadFactory, retryConfig, storage, upload);
            return new Analytics(uploadQueue, upload, storageQueue, storage, retry);
        }
    }
}
