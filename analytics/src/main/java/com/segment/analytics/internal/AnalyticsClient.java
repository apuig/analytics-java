package com.segment.analytics.internal;

import static com.segment.analytics.Log.Level.DEBUG;
import static com.segment.analytics.Log.Level.ERROR;
import static com.segment.analytics.Log.Level.VERBOSE;

import com.google.gson.Gson;
import com.segment.analytics.Log;
import com.segment.analytics.http.SegmentService;
import com.segment.analytics.http.UploadResponse;
import com.segment.analytics.messages.Batch;
import com.segment.analytics.messages.Message;
import dev.failsafe.CircuitBreaker;
import dev.failsafe.CircuitBreakerOpenException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.retrofit.FailsafeCall;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.HttpUrl;
import retrofit2.Call;
import retrofit2.Response;

public class AnalyticsClient {
  private static final Map<String, ?> CONTEXT;
  private static final int BATCH_MAX_SIZE = 1024 * 500;
  private static final Charset ENCODING = StandardCharsets.UTF_8;
  private Gson gsonInstance;
    private static final String instanceId = UUID.randomUUID().toString(); // TODO configurable ?

  static {
    Map<String, String> library = new LinkedHashMap<>();
    library.put("name", "analytics-java");
    library.put("version", AnalyticsVersion.get());
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("library", Collections.unmodifiableMap(library));
    context.put("instanceId", instanceId);
    CONTEXT = Collections.unmodifiableMap(context);
  }

  private final BlockingQueue<Message> messageQueue;
  private final HttpUrl uploadUrl;
  private final SegmentService service;
  private final int flushQueueSize;
    private final long flushIntervalInMillis;
  private final Log log;
  private final ExecutorService networkExecutor;
    private final Thread looperThread;
  private final AtomicBoolean isShutDown;
  private final String writeKey;
    private final FailsafeExecutor<Response<UploadResponse>> failsafe;
    private final FallbackAppender fallback;

  public static AnalyticsClient create(
      HttpUrl uploadUrl,
      SegmentService segmentService,
      int queueCapacity,
      int flushQueueSize,
      long flushIntervalInMillis,
      Log log,
      ThreadFactory threadFactory,
      ExecutorService networkExecutor,
      String writeKey,
      Gson gsonInstance) {
    return new AnalyticsClient(
        new LinkedBlockingQueue<Message>(queueCapacity),
        uploadUrl,
        segmentService,
        flushQueueSize,
        flushIntervalInMillis,
        log,
        threadFactory,
        networkExecutor,
        new AtomicBoolean(false),
        writeKey,
        gsonInstance);
  }

  public AnalyticsClient(
      BlockingQueue<Message> messageQueue,
      HttpUrl uploadUrl,
      SegmentService service,
      int flushQueueSize,
      long flushIntervalInMillis,
      Log log,
      ThreadFactory threadFactory,
      ExecutorService networkExecutor,
      AtomicBoolean isShutDown,
      String writeKey,
      Gson gsonInstance) {
    this.messageQueue = messageQueue;
    this.uploadUrl = uploadUrl;
    this.service = service;
    this.flushQueueSize = flushQueueSize;
        this.flushIntervalInMillis = flushIntervalInMillis;
    this.log = log;
    this.looperThread = threadFactory.newThread(new Looper());
    this.networkExecutor = networkExecutor;
    this.isShutDown = isShutDown;
    this.writeKey = writeKey;
    this.gsonInstance = gsonInstance;
        looperThread.start();

        CircuitBreaker<Response<UploadResponse>> breaker = CircuitBreaker.<Response<UploadResponse>>builder()
                // 10 failure in 2 minute open the circuit
                .withFailureThreshold(10, Duration.ofMinutes(2))
                // once open wait 30 seconds to be half-open
                .withDelay(Duration.ofSeconds(30))
                // after 1 success the circuit is closed
                .withSuccessThreshold(1)
                // 5xx or rate limit is an error
                .handleResultIf(response -> is5xx(response.code()) || response.code() == 429)
                .onOpen(el -> System.err.println("***\nOPEN\n***"))
                .onHalfOpen(el -> System.err.println("***\nHALF OPEN\n***"))
                .onClose(el -> System.err.println("***\nCLOSED\n***"))
                .build();

        RetryPolicy<Response<UploadResponse>> retry = RetryPolicy.<Response<UploadResponse>>builder()
                .withMaxAttempts(5)
                .withBackoff(1, 300, ChronoUnit.SECONDS)
                .withJitter(.2)
                // retry on IOException
                .handle(IOException.class)
                // retry on 5xx or rate limit
                .handleResultIf(response -> is5xx(response.code()) || response.code() == 429)
                .build();

        this.failsafe = Failsafe.with(retry, breaker).with(networkExecutor);
        this.fallback = new FallbackAppender(this);
  }

  public int messageSizeInBytes(Message message) {
    String stringifiedMessage = gsonInstance.toJson(message);

    return stringifiedMessage.getBytes(ENCODING).length;
  }

  public boolean offer(Message message) {
    return messageQueue.offer(message);
  }

    public void enqueue(Message message) {
        if (isShutDown.get()) {
            log.print(ERROR, "Attempt to enqueue a message when shutdown has been called %s.", message);
            return;
        }
        if (!messageQueue.offer(message)) {
            handleError(message);
        }
        else {
            System.err.println("enqueued " + message.messageId());
        }
    }
    

    // FIXME closeable
  public void shutdown() {
    if (isShutDown.compareAndSet(false, true)) {
      final long start = System.currentTimeMillis();

      // first let's tell the system to stop
      looperThread.interrupt();
      fallback.close();

      shutdownAndWait(networkExecutor, "network");

      log.print(
          VERBOSE, "Analytics client shut down in %s ms", (System.currentTimeMillis() - start));
    }
  }

    private void shutdownAndWait(ExecutorService executor, String name) {
    try {
      executor.shutdown();
      final boolean executorTerminated = executor.awaitTermination(1, TimeUnit.SECONDS);

      log.print(
          VERBOSE,
          "%s executor %s.",
          name,
          executorTerminated ? "terminated normally" : "timed out");
    } catch (InterruptedException e) {
      log.print(ERROR, e, "Interrupted while stopping %s executor.", name);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Looper runs on a background thread and takes messages from the queue. Once it collects enough
   * messages, it triggers a flush.
   */
  class Looper implements Runnable {

    public Looper() {
    }

    @Override
    public void run() {
      LinkedList<Message> messages = new LinkedList<>();
      int currentBatchSize = 0;
      boolean batchSizeLimitReached = false;
      int contextSize = gsonInstance.toJson(CONTEXT).getBytes(ENCODING).length;
      try {
          while (!Thread.currentThread().isInterrupted()) {
          Message message = messageQueue.poll(flushIntervalInMillis, TimeUnit.MILLISECONDS);

          if (message != null) {           
            // we do  +1 because we are accounting for this new message we just took from the queue
            // which is not in list yet
            // need to check if this message is going to make us go over the limit considering
            // default batch size as well
            int defaultBatchSize =
                BatchUtility.getBatchDefaultSize(contextSize, messages.size() + 1);
            int msgSize = messageSizeInBytes(message);
            if (currentBatchSize  + msgSize + defaultBatchSize <= BATCH_MAX_SIZE) {
              messages.add(message);
              currentBatchSize+=msgSize;
            } else {
              // put message that did not make the cut this time back on the queue, we already took
              // this message if we dont put it back its lost
              // we take care of that after submitting the batch
              batchSizeLimitReached = true;
            }
          }
          
          if (messages.isEmpty()) {
              continue;
          }

          Boolean isBlockingSignal = message == null;
          Boolean isOverflow = messages.size() >= flushQueueSize;

          if (!messages.isEmpty() && (isOverflow || isBlockingSignal || batchSizeLimitReached)) {
            Batch batch = Batch.create(CONTEXT, new ArrayList<>(messages), writeKey);
            log.print(
                VERBOSE,
                "Batching %s message(s) into batch %s.",
                batch.batch().size(),
                batch.sequence());

                Call<UploadResponse> call = service.upload(uploadUrl, batch);
                FailsafeCall<UploadResponse> failsafeCall =
                        FailsafeCall.with(failsafe).compose(call);
                failsafeCall.executeAsync()
                .thenAccept(r -> {
                    if(is5xx(r.code()) || r.code() == 429) {
                        handleError(batch, null);
                    }
                })
                .exceptionally(t -> {
                    handleError(batch, t);
                    return null;
                });

            currentBatchSize = 0;
            messages.clear();
            if (batchSizeLimitReached) {
              // If this is true that means the last message that would make us go over the limit
              // was not added,
              // add it to the now cleared messages list so its not lost
              messages.add(message);
            }
            batchSizeLimitReached = false;
          }
        }
      } catch (InterruptedException e) {
        log.print(DEBUG, "Looper interrupted while polling for messages.");
                // XXX CANCEL UPLOAD
            } catch (Exception e) {
                e.printStackTrace();
            }
      // SEND pending
      log.print(VERBOSE, "Looper stopped");
    }
    
  }
  
  void handleError(Batch batch, Throwable t) {
      if(t instanceof CompletionException ) {
	  if(t.getCause() instanceof CircuitBreakerOpenException) {	      
	      System.err.println("OPEN"); 
	  }
      }
      for(Message msg : batch.batch()) {	  
	  fallback.add(msg);
      }
  }

    void handleError(Message msg) {
        fallback.add(msg);
  }

    private static boolean is5xx(int status) {
      return status >= 500 && status < 600;
  }
  public static class BatchUtility {

    /**
     * Method to determine what is the expected default size of the batch regardless of messages
     *
     * <p>Sample batch:
     * {"batch":[{"type":"alias","messageId":"fc9198f9-d827-47fb-96c8-095bd3405d93","timestamp":"Nov
     * 18, 2021, 2:45:07
     * PM","userId":"jorgen25","integrations":{"someKey":{"data":"aaaaa"}},"previousId":"foo"},{"type":"alias",
     * "messageId":"3ce6f88c-36cb-4991-83f8-157e10261a89","timestamp":"Nov 18, 2021, 2:45:07
     * PM","userId":"jorgen25",
     * "integrations":{"someKey":{"data":"aaaaa"}},"previousId":"foo"},{"type":"alias",
     * "messageId":"a328d339-899a-4a14-9835-ec91e303ac4d","timestamp":"Nov 18, 2021, 2:45:07 PM",
     * "userId":"jorgen25","integrations":{"someKey":{"data":"aaaaa"}},"previousId":"foo"},{"type":"alias",
     * "messageId":"57b0ceb4-a1cf-4599-9fba-0a44c7041004","timestamp":"Nov 18, 2021, 2:45:07 PM",
     * "userId":"jorgen25","integrations":{"someKey":{"data":"aaaaa"}},"previousId":"foo"}],
     * "sentAt":"Nov 18, 2021, 2:45:07 PM","context":{"library":{"name":"analytics-java",
     * "version":"3.1.3"}},"sequence":1,"writeKey":"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
     *
     * <p>total size of batch : 932
     *
     * <p>BREAKDOWN: {"batch":[MESSAGE1,MESSAGE2,MESSAGE3,MESSAGE4],"sentAt":"MMM dd, yyyy, HH:mm:ss
     * tt","context":CONTEXT,"sequence":1,"writeKey":"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
     *
     * <p>so we need to account for: 1 -message size: 189 * 4 = 756 2 -context object size = 55 in
     * this sample -> 756 + 55 = 811 3 -Metadata (This has the sent data/sequence characters) +
     * extra chars (these are chars like "batch":[] or "context": etc and will be pretty much the
     * same length in every batch -> size is 73 --> 811 + 73 = 884 (well 72 actually, char 73 is the
     * sequence digit which we account for in point 5) 4 -Commas between each message, the total
     * number of commas is number_of_msgs - 1 = 3 -> 884 + 3 = 887 (sample is 886 because the hour
     * in sentData this time happens to be 2:45 but it could be 12:45 5 -Sequence Number increments
     * with every batch created
     *
     * <p>so formulae to determine the expected default size of the batch is
     *
     * @return: defaultSize = messages size + context size + metadata size + comma number + sequence
     *     digits + writekey + buffer
     * @return
     */
    private static int getBatchDefaultSize(int contextSize, int currentMessageNumber) {
      // sample data: {"batch":[],"sentAt":"MMM dd, yyyy, HH:mm:ss tt","context":,"sequence":1,
      //   "writeKey":"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"} - 119
      // Don't need to squeeze everything possible into a batch, adding a buffer
      int metadataExtraCharsSize = 119 + 1024;
      int commaNumber = currentMessageNumber - 1;

      return contextSize
          + metadataExtraCharsSize
          + commaNumber
          + String.valueOf(Integer.MAX_VALUE).length();
    }
  }
}
