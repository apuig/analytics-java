package com.segment.analytics.internal;

import com.segment.analytics.dto.Message;

public class MessageWithSize {

    final Message message;
    final int size;

    public MessageWithSize(final Message message, final int size) {
        this.message = message;
        this.size = size;
    }

    public Message getMessage() {
        return message;
    }

    public int getSize() {
        return size;
    }
}
