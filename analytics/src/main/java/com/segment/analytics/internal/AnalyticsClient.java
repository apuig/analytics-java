package com.segment.analytics.internal;

import static com.segment.analytics.Log.Level.DEBUG;
import static com.segment.analytics.Log.Level.ERROR;
import static com.segment.analytics.Log.Level.VERBOSE;

import com.google.gson.Gson;
import com.segment.analytics.Log;
import com.segment.analytics.http.SegmentService;
import com.segment.analytics.http.UploadResponse;
import com.segment.analytics.internal.Config.FileConfig;
import com.segment.analytics.internal.Config.HttpConfig;
import com.segment.analytics.messages.Batch;
import com.segment.analytics.messages.Message;
import dev.failsafe.CircuitBreaker;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import retrofit2.Response;

public class AnalyticsClient implements Closeable {
  private static final Logger LOGGER = Logger.getLogger(AnalyticsClient.class.getName());

  private static final Map<String, ?> CONTEXT;
  private static final int BATCH_MAX_SIZE = 1024 * 500;
  private static final int MSG_MAX_SIZE = 1024 * 32;
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

  private final HttpConfig config;
  private final BlockingQueue<Message> messageQueue;
  private final HttpUrl uploadUrl;
  private final SegmentService service;
  private final Log log;
  private final ExecutorService networkExecutor;
  private final String writeKey;
  private final Thread looperThread;
  private final AtomicBoolean isShutDown = new AtomicBoolean(false);
  private final CircuitBreaker<?> breaker;
  private final FallbackAppender fallback;
  private final ResubmitCheck resubmit;

  public AnalyticsClient(HttpUrl uploadUrl, SegmentService service, Log log, ThreadFactory threadFactory,
      String writeKey, Gson gsonInstance, HttpConfig config, FileConfig fileConfig)
      throws IOException {
    this.config = config;
    this.messageQueue = new LinkedBlockingQueue<Message>(config.queueSize);
    this.uploadUrl = uploadUrl;
    this.service = service;
    this.log = log;
    this.looperThread = threadFactory.newThread(new Looper());
    this.looperThread.setName(AnalyticsClient.class.getSimpleName() + "-Looper");
    this.networkExecutor = config.executor;
    this.writeKey = writeKey;
    this.gsonInstance = gsonInstance;

    this.breaker = CircuitBreaker.<Response<UploadResponse>>builder()
	// X failure in 1 minute open the circuit
	.withFailureThreshold(config.circuitErrorsInAMinute, Duration.ofMinutes(1))
	// once open wait X seconds to be half-open
	.withDelay(Duration.ofSeconds(config.circuitSecondsInOpen))
	// after X success the circuit is closed
	.withSuccessThreshold(config.circuitRequestToClose)
	// 5xx or rate limit is an error
	.handleResultIf(response -> is5xx(response.code()) || response.code() == 429)
	.onOpen(el -> LOGGER.log(Level.INFO, "OPEN: failing requests"))
	.onHalfOpen(el -> LOGGER.log(Level.INFO, "HALF OPEN: checking status"))
	.onClose(el -> LOGGER.log(Level.INFO, "CLOSED: attending requests normally")).build();

    this.fallback = new FallbackAppender(gsonInstance, threadFactory, fileConfig);
    this.resubmit = new ResubmitCheck(threadFactory, fileConfig, this);

    looperThread.start();
  }

  public int messageSizeInBytes(Message message) {
    String stringifiedMessage = gsonInstance.toJson(message);
    return stringifiedMessage.getBytes(ENCODING).length;
  }

  public boolean offer(Message message) throws IllegalArgumentException {
    if (messageSizeInBytes(message) > MSG_MAX_SIZE) {
      throw new IllegalArgumentException("Message was above individual limit. MessageId: " + message.messageId());
    }

    return messageQueue.offer(message);
  }

  public void enqueue(Message message) throws IllegalArgumentException {
    if (isShutDown.get()) {
      log.print(ERROR, "Attempt to enqueue a message when shutdown has been called %s.", message);
      return;
    }
    if (!offer(message)) {
      fallback.add(message);
    } else {
      LOGGER.log(Level.FINE, "enqueued {0}", message.messageId());
    }
  }

  @Override
  public void close() {
    if (isShutDown.compareAndSet(false, true)) {
      final long start = System.currentTimeMillis();

      // first let's tell the system to stop
      looperThread.interrupt();
      fallback.close();
      resubmit.close();

      shutdownAndWait(networkExecutor, "network");

      log.print(VERBOSE, "Analytics client shut down in %s ms", (System.currentTimeMillis() - start));
    }
  }

  private void shutdownAndWait(ExecutorService executor, String name) {
    try {
      executor.shutdown();
      final boolean executorTerminated = executor.awaitTermination(1, TimeUnit.SECONDS);

      log.print(VERBOSE, "%s executor %s.", name, executorTerminated ? "terminated normally" : "timed out");
    } catch (InterruptedException e) {
      log.print(ERROR, e, "Interrupted while stopping %s executor.", name);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Looper runs on a background thread and takes messages from the queue. Once it
   * collects enough messages, it triggers a flush.
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

      long reportedAt = System.currentTimeMillis();
      try {
	while (!Thread.currentThread().isInterrupted()) {
	  Message message = messageQueue.poll(config.flushIntervalInMillis, TimeUnit.MILLISECONDS);

	  if (message != null) {
	    // we do +1 because we are accounting for this new message we just took from the
	    // queue
	    // which is not in list yet
	    // need to check if this message is going to make us go over the limit
	    // considering
	    // default batch size as well
	    int defaultBatchSize = BatchUtility.getBatchDefaultSize(contextSize, messages.size() + 1);
	    int msgSize = messageSizeInBytes(message);
	    if (currentBatchSize + msgSize + defaultBatchSize <= BATCH_MAX_SIZE) {
	      messages.add(message);
	      currentBatchSize += msgSize;
	    } else {
	      // put message that did not make the cut this time back on the queue, we already
	      // took
	      // this message if we dont put it back its lost
	      // we take care of that after submitting the batch
	      batchSizeLimitReached = true;
	    }
	  }

	  if (messages.isEmpty()) {
	    continue;
	  }

	  Boolean isBlockingSignal = message == null;
	  Boolean isOverflow = messages.size() >= config.flushQueueSize;

	  if (!messages.isEmpty() && (isOverflow || isBlockingSignal || batchSizeLimitReached)) {
	    Batch batch = Batch.create(CONTEXT, new ArrayList<>(messages), writeKey);
	    log.print(VERBOSE, "Batching %s message(s) into batch %s.", batch.batch().size(), batch.sequence());

	    networkExecutor.submit(new UploadBatchTask(breaker, service, uploadUrl, batch,  fallback));

	    currentBatchSize = 0;
	    messages.clear();
	    if (batchSizeLimitReached) {
	      // If this is true that means the last message that would make us go over the
	      // limit
	      // was not added,
	      // add it to the now cleared messages list so its not lost
	      messages.add(message);
	    }
	    batchSizeLimitReached = false;
	  }

	  long now = System.currentTimeMillis();
	  if (now - reportedAt > 2_000) {
	    LOGGER.log(Level.FINE, "HTTPQueue: {0}", messageQueue.size());
	    if (networkExecutor instanceof ThreadPoolExecutor) {
	      ThreadPoolExecutor tpe = (ThreadPoolExecutor) networkExecutor;
	      LOGGER.log(Level.FINE, "HTTPPool active:{0}", tpe.getActiveCount());
	    }
	    reportedAt = now;
	  }
	}
      } catch (InterruptedException e) {
	log.print(DEBUG, "Looper interrupted while polling for messages.");
	Thread.currentThread().interrupt();
      }

      isShutDown.compareAndSet(false, true);

      Message msg = messageQueue.poll();
      while (msg != null) {
	fallback.add(msg);
	msg = messageQueue.poll();
      }

      log.print(VERBOSE, "Looper stopped");
    }
  }

  private static boolean is5xx(int status) {
    return status >= 500 && status < 600;
  }

  static interface SupplierWithException<T> {
    T get() throws Exception;
  }


  
  static abstract class UploadTask implements Runnable{
      final CircuitBreaker<?> breaker;
      final SegmentService service;
      final HttpUrl uploadUrl;
    public UploadTask(CircuitBreaker<?> breaker, SegmentService service, HttpUrl uploadUrl) {
        this.breaker = breaker;
        this.service = service;
        this.uploadUrl = uploadUrl;
    }

        boolean upload(SupplierWithException<Response<UploadResponse>> uploadRequest) {
            if (breaker.tryAcquirePermit()) {
                try {
                    Response<UploadResponse> upload = uploadRequest.get();
                    if (upload.isSuccessful()) {
                        breaker.recordSuccess();
                        // FIXME handle response ? do not retry those ?
                        // upload.body().success())
                        return true;
                    } else if (upload.code() == 429) {
                        breaker.open();
                    } else {
                        breaker.recordFailure();
                }
                } catch (Exception e) {
                    breaker.recordException(e);
            }
        }
            return false;
        }
  }
  
  static class UploadBatchTask extends UploadTask {

    final Batch batch;
    final FallbackAppender fallback;

    UploadBatchTask(final CircuitBreaker<?> breaker, final SegmentService service, final HttpUrl uploadUrl, final Batch batch
	, FallbackAppender fallback) {
      super(breaker, service, uploadUrl);
      this.batch = batch;
      this.fallback = fallback;
    }

    @Override
    public void run() {
      if (!upload(() -> service.upload(uploadUrl, batch).execute())) {
	fallback.add(batch);
      }
    }
  }

  static class UploadFileTask extends UploadTask {
    final Path path;

    static final MediaType JSON = MediaType.get("application/json");

    UploadFileTask(final CircuitBreaker<?> breaker, final SegmentService service, final HttpUrl uploadUrl, final Path path) {
      super(breaker, service, uploadUrl);
      this.path = path;
    }

    @Override
    public void run() {
      try {
        if(Files.lines(path).map(batchLine -> upload(() -> service.upload(uploadUrl, RequestBody.create(batchLine, JSON)).execute())).allMatch(Boolean.TRUE::equals)) {
          try {
            Files.delete(path);
          } catch (IOException e) {
            // will attempt to submit again (rename file?)
            LOGGER.log(Level.WARNING, "Cannot delete file " + path, e);
          }
        }
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Cannot process file " + path, e);
      }
    }
  }

  public void resubmit(Path path) throws IOException {
    networkExecutor.submit(new UploadFileTask(breaker, service, uploadUrl, path));
  }

  public static class BatchUtility {

    private static int getBatchDefaultSize(int contextSize, int currentMessageNumber) {
      // sample data: {"batch":[],"sentAt":"MMM dd, yyyy, HH:mm:ss
      // tt","context":,"sequence":1,
      // "writeKey":"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"} - 119
      // Don't need to squeeze everything possible into a batch, adding a buffer
      int metadataExtraCharsSize = 119 + 1024;
      int commaNumber = currentMessageNumber - 1;

      return contextSize + metadataExtraCharsSize + commaNumber + String.valueOf(Integer.MAX_VALUE).length();
    }
  }
}
