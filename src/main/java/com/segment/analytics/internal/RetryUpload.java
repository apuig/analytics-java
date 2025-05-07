package com.segment.analytics.internal;

import com.segment.analytics.config.RetryConfig;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodic check to upload pending files. If it succeed the file is deleted, otherwise use {@link RetriableFile} to reschedule for a latter run.
 */
public class RetryUpload implements Closeable, Runnable {
    private static final Logger LOGGER = Logger.getLogger(RetryUpload.class.getName());

    private final Storage storage;
    private final Upload upload;
    private final ScheduledExecutorService executor;

    public RetryUpload(ThreadFactory threadFactory, RetryConfig retryConfig, Storage storage, Upload upload) {
        this.storage = storage;
        this.upload = upload;
        this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = threadFactory.newThread(r);
                t.setName("segment-retry");
                return t;
            }
        });
        this.executor.scheduleWithFixedDelay(
                this, retryConfig.initialDelaySeconds, retryConfig.delaySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        LOGGER.log(Level.INFO, "retry started");
        Queue<FileEntry> orderedFiles = storage.listFilesWithRetryAfterNow(1_000);
        while (!orderedFiles.isEmpty()) {
            FileEntry entry = orderedFiles.poll();
            Path tmpFile = storage.tryMoveToTmp(entry.file);
            if (tmpFile != null) {
                // XXX may use the calling thread (networkExecutor config)
                upload.retry(tmpFile, storage::tryDelete, storage::handleRetry);
            }
        }
        LOGGER.log(Level.INFO, "retry completed");
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.log(Level.SEVERE, "retry check not canceled in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
