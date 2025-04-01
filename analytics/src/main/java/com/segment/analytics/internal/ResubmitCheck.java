package com.segment.analytics.internal;

import com.segment.analytics.internal.Config.FileConfig;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO this class, somehow, should keep track of the number of retries. All the filenames start with the creation time 
public class ResubmitCheck implements Closeable, Runnable {
  private static final Logger LOGGER = Logger.getLogger(ResubmitCheck.class.getName());

  private final Path directory;
  private final AnalyticsClient client;
  private final ScheduledExecutorService executor;

  public ResubmitCheck(ThreadFactory threadFactory, FileConfig config, AnalyticsClient client) {
    this.directory = Path.of(config.filePath);
    this.client = client;
    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
	Thread t = threadFactory.newThread(r);
	t.setName(ResubmitCheck.class.getSimpleName());
	return t;
      }
    });
    this.executor.scheduleWithFixedDelay(this, 0, 1, TimeUnit.MINUTES);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

  @Override
  public void run() {
    try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, entry -> Files.isRegularFile(entry)
	&& !entry.getFileName().toString().endsWith(FallbackAppender.TMP_EXTENSION))) {
      Iterator<Path> fileIterator = files.iterator();
      while (fileIterator.hasNext()) {
	Path file = fileIterator.next();

	// FIXME filter by instance
	client.resubmit(file);
	// FIXME use calling thread policy executor
      }
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Cannot list directory " + directory, e);
    }
  }
}
