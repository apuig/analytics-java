package com.segment.analytics;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

// Helper log handler for capturing logs
public class TestLogHandler extends Handler {
    public List<LogRecord> records = new ArrayList<>();

    @Override
    public void publish(final LogRecord r) {
        records.add(r);
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() throws SecurityException {
        // no-op
    }
}
