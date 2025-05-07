package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.IdentifyMessage;
import com.segment.analytics.dto.Message;
import com.segment.analytics.dto.TrackMessage;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import org.junit.Test;

public class JSONTest {

    @Test
    public void messageType() throws Throwable {
        final Batch batch = new Batch();
        batch.setBatch(new LinkedList<Message>());
        batch.getBatch().add(new IdentifyMessage("u", null));
        batch.getBatch().add(new TrackMessage("u", "e"));

        final Batch batchBack = JSON.objectMapper.readValue(JSON.toJson(batch), Batch.class);

        assertThat(batchBack.getBatch()).hasSize(2);
        assertThat(batchBack.getBatch().get(0)).isInstanceOf(IdentifyMessage.class);
        assertThat(batchBack.getBatch().get(1)).isInstanceOf(TrackMessage.class);
    }

    @Test
    public void instant() throws Throwable {
        final Batch batch = new Batch();
        assertThat(batch.getSentAt()).isNotNull();
        // segment expects millisecond resolution
        final Instant withMillis = batch.getSentAt().truncatedTo(ChronoUnit.MILLIS);

        final Batch batchBack = JSON.objectMapper.readValue(JSON.toJson(batch), Batch.class);
        assertThat(batchBack.getSentAt()).isEqualTo(withMillis);
    }

    @Test
    public void uncheckedExceptionToJson() {
        final Object badObj = new BadSerialization();
        assertThatThrownBy(() -> JSON.toJson(badObj)).isInstanceOf(UncheckedIOException.class);
    }

    @Test
    public void uncheckedExceptionWrite() {
        final Batch b = new Batch() {
            @Override
            public Instant getSentAt() {
                throw new RuntimeException("");
            }
        };
        final StringWriter sw = new StringWriter();
        assertThatThrownBy(() -> JSON.write(b, sw)).isInstanceOf(UncheckedIOException.class);
    }

    static class BadSerialization {
        String getValue() {
            throw new RuntimeException("");
        }
    }
}
