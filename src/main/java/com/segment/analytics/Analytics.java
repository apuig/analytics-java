package com.segment.analytics;

import com.segment.analytics.config.BatchQueueConfig;
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

/***
 * An analytics client to send messages to Segment.
 * <p>
 * The client batches messages and sends them to Segment asynchronously, honoring the following limits:
 * <ul>
 * <li>configurable number of messages in a batch</li>
 * <li>configurable time a message can stay in the queue before being sent</li>
 * <li>size of batch: 500KB</li>
 * </ul>
 * <p>
 * When the HTTP client cannot send messages to Segment it will store them on disk and retry later, see {@link RetryUpload}.
 * </p>
 * <p>
 * The client should be closed when it is no longer needed. The client is thread-safe and can be used from multiple threads.
 * </p>
 * <p>
 * Example:
 * <pre>
 * Analytics analytics = Analytics.builder
 * 	.writeKey("YOUR_WRITE_KEY")
 *	.build();
 * analytics.enqueue(new TrackMessage("userId", "event"));
 * analytics.close();
 * </pre>
 * </p>
 */
public class Analytics implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(Analytics.class.getName());

    private final AtomicBoolean closed = new AtomicBoolean(false);
    protected final BatchQueue uploadQueue;
    private final Upload upload;
    protected final BatchQueue storageQueue;
    private final Storage storage;
    private final RetryUpload retry;

    Analytics(
            final BatchQueue uploadQueue,
            final Upload upload,
            final BatchQueue storageQueue,
            final Storage storage,
            final RetryUpload retry) {
        this.uploadQueue = uploadQueue;
        this.upload = upload;
        this.storageQueue = storageQueue;
        this.storage = storage;
        this.retry = retry;
    }

    Analytics(final Upload upload, final Storage storage, final Builder builder, final Map<String, ?> batchContext) {
        this(
                new BatchQueue(builder.threadFactory, builder.writeKey, batchContext, builder.httpConfig, upload),
                upload,
                new BatchQueue(builder.threadFactory, builder.writeKey, batchContext, builder.storageConfig, storage),
                storage,
                new RetryUpload(builder.threadFactory, builder.retryConfig, storage, upload));
    }

    /**
     * Enqueue a message to be sent to Segment.
     * <p>
     * The message is first enqueued in memory to be uploaded by the HTTP client, if this queue is full, it is enqueued to disk.
     * If the disk queue is also full, the message is dropped, see {@link BatchQueueConfig#blockTimeout}.
     *
     * @param message the message to enqueue
     * @throws IllegalArgumentException if the message has no messageId, or its size is >32Kb
     */
    public void enqueue(final Message message) throws IllegalArgumentException {
        if (closed.get()) {
            LOGGER.log(Level.WARNING, () -> "Attempt to enqueue a message when shutdown has been called " + message);
            return;
        }
        if (message.getMessageId() == null) {
            throw new IllegalArgumentException("Message without messageId");
        }
        final int size = JSON.sizeInBytes(message);
        if (size > Constants.MSG_MAX_SIZE) {
            throw new IllegalArgumentException(
                    "Message was above individual limit. MessageId: " + message.getMessageId());
        }
        final MessageWithSize mws = new MessageWithSize(message, size);
        if (uploadQueue.put(mws)) {
            LOGGER.log(Level.FINEST, () -> "Enqueued HTTP " + message.getMessageId());
        } else if (storageQueue.put(mws)) {
            LOGGER.log(Level.FINEST, () -> "Enqueued Storage " + message.getMessageId());
        } else {
            LOGGER.log(Level.WARNING, () -> "Lost overflow event (queue full): " + message.getMessageId());
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            retry.close();
            uploadQueue.close();
            upload.close();

            for (final MessageWithSize msg : uploadQueue.drainQueue()) {
                storageQueue.put(msg);
            }
            storageQueue.close();
            for (final MessageWithSize msg : storageQueue.drainQueue()) {
                LOGGER.log(
                        Level.WARNING,
                        () -> "Lost overflow event: "
                                + new String(JSON.toJson(msg.getMessage()), StandardCharsets.UTF_8));
            }
            storage.close();
        }
    }

    /***
     * Create a builder with the given write key.
     * @param writeKey your Segment write key
     */
    public static Builder builder(final String writeKey) {
        if ((writeKey == null) || writeKey.isEmpty() || (writeKey.length() > 32)) {
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

        Builder(final String writeKey) {
            this.writeKey = writeKey;
        }

        public Builder endpoint(final String endpoint) throws URISyntaxException {
            if ((endpoint == null) || endpoint.isBlank()) {
                throw new IllegalArgumentException("endpoint cannot be null or blank.");
            }
            this.uri = new URI(endpoint + Defaults.DEFAULT_PATH);
            return this;
        }

        public Builder httpConfig(final HttpConfig httpConfig) {
            this.httpConfig = httpConfig;
            return this;
        }

        public Builder storageConfig(final StorageConfig storageConfig) {
            this.storageConfig = storageConfig;
            return this;
        }

        public Builder retryConfig(final RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }

        /***
         * Additional application specific context to be added to each batch.
         * @param context additional context
         */
        public Builder context(final Map<String, ?> context) {
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

            final Map<String, Object> batchContext = new HashMap<>();
            if (context != null) {
                batchContext.putAll(context);
            }
            batchContext.put("library", Map.of("name", "analytics-java", "version", Constants.VERSION));

            final Storage storage = new Storage(retryConfig, storageConfig);
            final Upload upload = new Upload(httpConfig, uri, storage);
            return new Analytics(upload, storage, this, batchContext);
        }
    }
}
