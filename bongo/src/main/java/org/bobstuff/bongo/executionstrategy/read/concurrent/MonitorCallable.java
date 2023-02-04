package org.bobstuff.bongo.executionstrategy.read.concurrent;

import static org.bobstuff.bongo.BongoDbBatchBlockingCursor.END_RESPONSE;

import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.messages.BongoFindResponse;

@Slf4j
public class MonitorCallable<TModel> implements Callable<Void> {
  private int parserCount;
  private Future<Void> fetcher;
  private CompletionService<Void> parserCompletionService;
  private BlockingQueue<BobBsonBuffer> decodeQueue;
  private BlockingQueue<BongoFindResponse<TModel>> responses;

  public MonitorCallable(
      int parserCount,
      Future<Void> fetcher,
      CompletionService<Void> parserCompletionService,
      BlockingQueue<BobBsonBuffer> decodeQueue,
      BlockingQueue<BongoFindResponse<TModel>> responses) {
    this.parserCount = parserCount;
    this.fetcher = fetcher;
    this.parserCompletionService = parserCompletionService;
    this.decodeQueue = decodeQueue;
    this.responses = responses;
  }

  @Override
  public Void call() throws Exception {
    while (true) {
      try {
        fetcher.get(100, TimeUnit.MILLISECONDS);
        break;
      } catch (TimeoutException e) {
        log.trace("Timeout waiting for fetcher to complete");
      } catch (InterruptedException e) {
        return null;
      } catch (BongoException e) {
        // TODO what to do?  Do we mark this strategy as not usable since tasks maybe frozen and we
        // need to stop them
        throw e;
      }

      var result = parserCompletionService.poll();
      if (result != null) {
        // TODO how do we trigger an abort?
      }
    }
    if (Thread.interrupted()) {
      return null;
    }
    log.debug("fetcher is complete adding the end response onto the queue");
    for (var i = 0; i < parserCount; i += 1) {
      decodeQueue.put(DecodingCallable.POISON_BUFFER);
    }

    int completeCount = 0;
    while (completeCount < parserCount) {
      parserCompletionService.take().get();
      completeCount += 1;
    }
    log.debug("parser is complete adding the end response onto the queue");
    responses.put((BongoFindResponse) END_RESPONSE);

    return null;
  }

  @SuppressWarnings("unchecked")
  private void submitResponseQueuePoisonPill() {
    try {
      responses.put((BongoFindResponse<TModel>) END_RESPONSE);
    } catch (InterruptedException e) {
      throw new BongoException(
          "Failed to put poison pill onto responses queue due to interrupt", e);
    }
  }
}
