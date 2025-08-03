package com.segment.analytics.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.segment.analytics.config.BatchQueueConfig;
import com.segment.analytics.config.Constants;
import com.segment.analytics.config.Defaults;
import com.segment.analytics.dto.Batch;
import com.segment.analytics.dto.IdentifyMessage;
import com.segment.analytics.dto.Message;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.assertj.core.data.Offset;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

public class BatchQueueTest {

    private static final String WK = "writeKey";
    private static final Map<String, ?> CONTEXT = Map.of("foo", 1, "bar", List.of("a", "b"));

    List<Batch> batches;
    Consumer<Batch> batchConsumer;

    @Before
    public void setup() {
        batches = new ArrayList<>();
        batchConsumer = batches::add;
    }

    @Test
    public void flushMs() throws Throwable {
        // Given flushMs is X
        // flushSize and size high enough to only trigger flushMs
        int flushSize = 300;
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(100)
                        .flushSize(100)
                        .flushMs(flushSize)
                        .build(),
                batchConsumer)) {

            // When put 1 messages
            AtomicBoolean first = new AtomicBoolean(true);

            // Then the batch is created within flushMs
            Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .atLeast(Duration.ofMillis(flushSize))
                    .atMost(Duration.ofMillis(flushSize * 2))
                    .until(() -> {
                        if (first.compareAndSet(true, false)) {
                            bq.put(createIdentifyMessage());
                        }
                        return !batches.isEmpty();
                    });

            // And no more messages in the queue
            assertThat(bq.isEmpty()).isTrue();

            // And the created batch messages respect the creation time of batch
            Batch batch = assertThat(batches).singleElement().actual();
            Instant batchTime = batch.getSentAt();
            Instant messageTime =
                    assertThat(batch.getBatch()).singleElement().actual().getTimestamp();

            long additionalMs = Duration.between(messageTime, batchTime)
                    .minus(Duration.ofMillis(flushSize))
                    .toMillis();
            assertThat(additionalMs).isCloseTo(0l, Offset.offset(100l));
            // Also check batch context propagation
            assertThat(batch.getContext()).isEqualTo(CONTEXT);
            // Also check batch writeKey propagation
            assertThat(batch.getWriteKey()).isEqualTo(WK);
        }
    }

    @Test
    public void flushSize() throws Throwable {
        // Given flushSize is X
        // flushMs and size high enough to only trigger flushSize
        int flushSize = 3;
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(flushSize * 2)
                        .flushSize(flushSize)
                        .flushMs(Integer.MAX_VALUE)
                        .build(),
                batchConsumer)) {

            // When put X -1 messages
            for (int i = 1; i < flushSize; i++) {
                bq.put(createIdentifyMessage());
            }

            // Then no batch is generated
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            assertThat(batches).isEmpty();

            // When put the X message
            bq.put(createIdentifyMessage());

            // Then the batch is created
            Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> !batches.isEmpty());
            // And no more messages in the queue
            assertThat(bq.isEmpty()).isTrue();

            // And the created batch contains X messages
            int batchSize =
                    assertThat(batches).singleElement().actual().getBatch().size();
            assertThat(batchSize).isEqualTo(flushSize);
            // Also check batch context propagation
            Batch batch = assertThat(batches).singleElement().actual();
            assertThat(batch.getContext()).isEqualTo(CONTEXT);
            assertThat(batch.getWriteKey()).isEqualTo(WK);
        }
    }

    @Test
    public void batchSize() throws Throwable {
        // Given constant of max batchSize
        // flushSize, flushMs and size high enough to only trigger batchSize
        int flushMs = 10_000;
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(50_000)
                        .flushSize(10_000)
                        .flushMs(flushMs)
                        .build(),
                batchConsumer)) {

            // AND a control batch to check the size
            Batch control = new Batch();
            control.setContext(CONTEXT);
            control.setWriteKey(WK);
            control.setBatch(new ArrayList<>());

            // When adding messages while the batch size is not reached
            while (true) {
                Message msg = createIdentifyMessage();
                control.getBatch().add(msg);

                int controlSize = JSON.sizeInBytes(control);
                if (controlSize > Constants.MAX_BATCH_SIZE) {
                    // When the next message will reach the limit

                    // Then no batch is generated
                    assertThat(batches).isEmpty();

                    // When adding the last message
                    bq.put(msg);

                    // Then the batch is created
                    Awaitility.await().atMost(Duration.ofMillis(flushMs * 2)).until(() -> !batches.isEmpty());

                    // And the created batch meet the limit
                    Batch batch = assertThat(batches).singleElement().actual();
                    assertThat(JSON.sizeInBytes(batch)).isLessThan((int) Constants.MAX_BATCH_SIZE);

                    // And the last message do not belong to the batch
                    assertThat(batch.getBatch()).noneMatch(b -> Objects.equals(b.getMessageId(), msg.getMessageId()));

                    // AND the message can be obtained after close
                    bq.close();
                    assertThat(bq.drainQueue())
                            .singleElement()
                            .matches(mws -> Objects.equals(mws.message.getMessageId(), msg.getMessageId()));

                    return;
                } else {
                    bq.put(msg);
                }
            }
        }
    }

    @Test
    public void stopAndDrain() throws Throwable {
        // Given large flushMs and flushSize
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(50_000)
                        .flushSize(10_000)
                        .flushMs(10_000)
                        .build(),
                batchConsumer)) {

            // When adding a message to the queue
            Message msg = createIdentifyMessage();
            bq.put(msg);

            // Then no batch is generated
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            assertThat(batches).isEmpty();

            // When closing
            bq.close();

            // Then the message can be consulted.
            assertThat(bq.drainQueue())
                    .singleElement()
                    .matches(mws -> Objects.equals(mws.message.getMessageId(), msg.getMessageId()));
            // Also check that drainQueue returns empty after draining
            assertThat(bq.drainQueue()).isEmpty();
        }
    }

    @Test
    public void offerNonBlocking() throws Throwable {
        // Given a small queue size and not starting the consumer
        int size = 2;
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(size)
                        .flushSize(size)
                        .flushMs(10000)
                        .build(),
                batchConsumer,
                false)) {

            // When offering messages up to capacity
            for (int i = 0; i < size; i++) {
                assertThat(bq.offer(createIdentifyMessage())).isTrue();
            }
            // Then offering one more, should return false
            assertThat(bq.offer(createIdentifyMessage())).isFalse();
        }
    }

    @Test
    public void putBlocksWhenFull() throws Throwable {
        // Given a queue size of 1 and and not starting the consumer
        int size = 1;
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(size)
                        .flushSize(size + 10)
                        .flushMs(Integer.MAX_VALUE)
                        .build(),
                batchConsumer,
                false)) {
            // and queue already contains 1 element
            bq.put(createIdentifyMessage());
            // When adding a new element
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean blocked = new AtomicBoolean(false);
            Thread t = new Thread(() -> {
                blocked.set(true);
                bq.put(createIdentifyMessage());
                latch.countDown();
            });
            t.start();
            // And wait to ensure thread is blocked on put
            Thread.sleep(500);
            // Then the queue put operation is blocked
            assertThat(blocked.get()).isTrue();
            assertThat(latch.getCount()).isEqualTo(1);
            bq.drainQueue();
            latch.await(1, TimeUnit.SECONDS);
            assertThat(latch.getCount()).isZero();
        }
    }

    @Test
    public void emptyQueueDrainReturnsEmpty() throws Throwable {
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder().size(10).flushSize(10).flushMs(10000).build(),
                batchConsumer)) {
            assertThat(bq.drainQueue()).isEmpty();
        }
    }

    @Test
    public void concurrentPutAndBatching() throws Throwable {
        int threadCount = 5;
        int messagesPerThread = 10;
        int totalMessages = threadCount * messagesPerThread;
        try (BatchQueue bq = new BatchQueue(
                Executors.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(totalMessages)
                        .flushSize(totalMessages)
                        .flushMs(10000)
                        .build(),
                batchConsumer)) {

            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    for (int j = 0; j < messagesPerThread; j++) {
                        bq.put(createIdentifyMessage());
                    }
                }));
            }
            threads.forEach(Thread::start);
            for (Thread t : threads) {
                t.join();
            }

            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> !batches.isEmpty());
            Batch batch = assertThat(batches).singleElement().actual();
            assertThat(batch.getBatch()).hasSize(totalMessages);
        }
    }

    @Test
    public void messageOrderingPreserved() throws Throwable {
        int flushSize = 5;
        List<String> ids = new ArrayList<>();
        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder()
                        .size(flushSize)
                        .flushSize(flushSize)
                        .flushMs(10000)
                        .build(),
                batchConsumer)) {

            for (int i = 0; i < flushSize; i++) {
                Message m = createIdentifyMessage();
                m.setMessageId("msg-" + i);
                ids.add(m.getMessageId());
                bq.put(m);
            }
            Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> !batches.isEmpty());
            Batch batch = assertThat(batches).singleElement().actual();
            List<String> batchIds =
                    batch.getBatch().stream().map(Message::getMessageId).toList();
            assertThat(batchIds).containsExactlyElementsOf(ids);
        }
    }

    @Test
    public void batchConsumerExceptionDoesNotCrashThread() throws Throwable {
        AtomicInteger callCount = new AtomicInteger(0);
        Consumer<Batch> faultyConsumer = b -> {
            callCount.incrementAndGet();
            throw new RuntimeException("fail");
        };

        try (BatchQueue bq = new BatchQueue(
                Defaults.defaultThreadFactory(),
                WK,
                CONTEXT,
                BatchQueueConfig.builder().size(10).flushSize(2).flushMs(10000).build(),
                faultyConsumer)) {

            bq.put(createIdentifyMessage());
            bq.put(createIdentifyMessage());
            Thread.sleep(200);
            // Thread should still be alive and able to accept more messages
            bq.put(createIdentifyMessage());
            bq.put(createIdentifyMessage());
            Thread.sleep(200);
            assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
        }
    }

    private static Message createIdentifyMessage() {
        Message m = new IdentifyMessage();
        m.setMessageId(UUID.randomUUID().toString());
        m.setUserId(UUID.randomUUID().toString());
        return m;
    }
}
