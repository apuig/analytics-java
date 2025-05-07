package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.segment.analytics.config.BatchQueueConfig;
import com.segment.analytics.config.Constants;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.Message;
import com.segment.analytics.dto.TrackMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.BeforeTry;
import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;

public class BatchQueuePropertyTest {

    List<Batch> batches;
    Consumer<Batch> batchConsumer;

    @BeforeTry
    void setup() {
        batches = new ArrayList<Batch>();
        batchConsumer = batches::add;
    }

    public static class MessageWithDelay {
        Message msg;
        int delay;

        public MessageWithDelay(Message msg, int delay) {
            this.msg = msg;
            this.delay = delay;
        }

        @Override
        public String toString() {
            return "delay:" + delay;
        }
    }

    @Provide
    Arbitrary<MessageWithDelay> messagesWithDelay() {
        return Arbitraries.integers().between(0, 200).filter(d -> d % 5 == 0).map((d) -> {
            Message m = new TrackMessage() {
                @Override
                public String toString() {
                    return "delay: " + d;
                }
            };
            m.setTimestamp(Instant.now());
            m.setMessageId("00000");
            m.setUserId("00000");
            return new MessageWithDelay(m, d);
        });
    }

    @Provide
    Arbitrary<List<MessageWithDelay>> messagesWithDelayList() {
        return messagesWithDelay().list().ofMinSize(100).ofMaxSize(500);
    }

    @Property(tries = 100)
    public void flushMs(
            @ForAll @IntRange(min = 100, max = 1_000) int flushMs,
            @ForAll("messagesWithDelayList") List<MessageWithDelay> messages)
            throws Exception {
        Assume.that(messages.stream().mapToInt(m -> m.delay).sum() > flushMs);

        double delayFactor = flushMs > 500 ? 1.1 : 1.7; // account some internal work
        int flushMsWithFactor = (int) (flushMs * delayFactor);

        final AtomicInteger expectedMsgCount = new AtomicInteger();
        BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                "wk",
                Map.of(),
                BatchQueueConfig.builder()
                        .size(5_000)
                        .flushSize(5_000)
                        .flushMs(flushMs)
                        .build(),
                batchConsumer);

        messages.forEach(m -> {
            if (m.delay != 0) {
                try {
                    Thread.sleep(m.delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            m.msg.setTimestamp(Instant.now());
            bq.put(m.msg);
            expectedMsgCount.incrementAndGet();
        });

        Thread.sleep(flushMsWithFactor);
        bq.close();

        assertThat(batches).isNotEmpty();
        int msgCount = 0;
        for (Batch b : batches) {
            assertThat(JSON.sizeInBytes(b)).isLessThanOrEqualTo(Constants.MAX_BATCH_SIZE);
            for (Message msg : b.getBatch()) {
                long time = Duration.between(msg.getTimestamp().plusMillis(flushMsWithFactor), b.getSentAt())
                        .toMillis();
                assertThat(time).isCloseTo(0l, Offset.offset((long) flushMsWithFactor));
            }
            msgCount += b.getBatch().size();
        }

        msgCount += bq.drainQueue().size();
        assertThat(msgCount).isEqualTo(expectedMsgCount.get());
    }

    @Provide
    Arbitrary<Message> messages() {
        Arbitrary<String> content = Arbitraries.integers()
                .between(1024, 28 * 1024)
                .map(numChars -> String.valueOf('a').repeat(numChars));

        return Combinators.combine(Arbitraries.create(() -> UUID.randomUUID().toString()), content)
                .as((uuid, cont) -> {
                    Message m = new TrackMessage() {
                        @Override
                        public String toString() {
                            return "numChars: " + cont.length();
                        }
                    };
                    m.setTimestamp(Instant.now());
                    m.setMessageId(uuid);
                    m.setUserId(uuid);
                    m.setContext(Map.of("content", cont));
                    return m;
                })
                .filter(m -> JSON.sizeInBytes(m) < Constants.MSG_MAX_SIZE);
    }

    @Provide
    Arbitrary<List<Message>> messageList() {
        return messages().list().ofMinSize(100).ofMaxSize(500);
    }

    @Property(tries = 100)
    public void flushSize(
            @ForAll @IntRange(min = 1, max = 400) int flushSize, @ForAll("messageList") List<Message> messages)
            throws Exception {
        Assume.that(messages.size() > flushSize);

        final AtomicInteger expectedMsgCount = new AtomicInteger();
        BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                "wk",
                Map.of(),
                BatchQueueConfig.builder()
                        .size(5_000)
                        .flushSize(flushSize)
                        .flushMs(500)
                        .build(),
                batchConsumer);

        messages.forEach(m -> {
            m.setTimestamp(Instant.now());
            bq.put(m);
            expectedMsgCount.incrementAndGet();
        });

        Awaitility.await()
                .pollInterval(Duration.ofMillis(50))
                .pollDelay(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(1))
                .until(bq::isEmpty);
        bq.close();

        assertThat(batches).isNotEmpty();
        int msgCount = 0;
        for (Batch b : batches) {
            assertThat(JSON.sizeInBytes(b)).isLessThanOrEqualTo(Constants.MAX_BATCH_SIZE);
            assertThat(b.getBatch().size()).isLessThanOrEqualTo(flushSize);
            msgCount += b.getBatch().size();
        }

        int drainSize = bq.drainQueue().size();
        assertThat(drainSize).isLessThan(flushSize);
        msgCount += drainSize;
        assertThat(msgCount).isEqualTo(expectedMsgCount.get());
    }
}
