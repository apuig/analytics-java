package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.IdentifyMessage;
import com.segment.analytics.dto.Message;
import com.segment.analytics.dto.TrackMessage;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import org.junit.Test;

public class JSONTest {

    @Test
    public void messageType() throws Throwable {
        Batch batch = new Batch();
        batch.setBatch(new LinkedList<Message>());
        batch.getBatch().add(new IdentifyMessage());
        batch.getBatch().add(new TrackMessage());

        Batch batchBack = JSON.OBJECT_MAPPER.readValue(JSON.toJson(batch), Batch.class);

        assertThat(batchBack.getBatch()).hasSize(2);
        assertThat(batchBack.getBatch().get(0)).isInstanceOf(IdentifyMessage.class);
        assertThat(batchBack.getBatch().get(1)).isInstanceOf(TrackMessage.class);
    }

    @Test
    public void instant() throws Throwable {
        Batch batch = new Batch();
        assertThat(batch.getSentAt()).isNotNull();
        // segment expects millisecond resolution
        Instant withMillis = batch.getSentAt().truncatedTo(ChronoUnit.MILLIS);

        Batch batchBack = JSON.OBJECT_MAPPER.readValue(JSON.toJson(batch), Batch.class);
        assertThat(batchBack.getSentAt()).isEqualTo(withMillis);
    }
}
