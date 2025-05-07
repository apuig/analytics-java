package com.segment.analytics.internal;

import com.segment.analytics.Analytics;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Accept and process a Batch. Implementations should handle possible errors and never throws.
 *  */
interface BatchConsumer<T> {
    Logger LOGGER = Logger.getLogger(Analytics.class.getName());

    void process(T batch) throws Exception;

    default void safeProcess(final T batch) {
        try {
            process(batch);
        } catch (final Exception e) {
            LOGGER.log(Level.WARNING, e, () -> "Unexpected error processing batch");
        }
    }
}
