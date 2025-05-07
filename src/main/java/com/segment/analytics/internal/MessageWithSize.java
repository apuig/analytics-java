package com.segment.analytics.internal;

import com.segment.analytics.dto.Message;

public class MessageWithSize {

    public final Message message;
    public final int size;

    public MessageWithSize(Message message, int size) {
        this.message = message;
        this.size = size;
    }
}
