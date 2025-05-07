package com.segment.analytics.internal;

import java.nio.file.Path;

class FileEntry implements Comparable<FileEntry> {
    final Path file;
    final long retryTime;

    protected FileEntry(final Path file, final long retryTime) {
        this.file = file;
        this.retryTime = retryTime;
    }

    @Override
    public int compareTo(final FileEntry other) {
        return Long.compare(this.retryTime, other.retryTime);
    }
}
