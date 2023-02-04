package org.bobstuff.bongo;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.executionstrategy.ReadExecutionStrategy;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class BongoDbBatchBlockingCursor<TModel> implements BongoDbBatchCursor<TModel> {
  private BlockingQueue<BongoFindResponse<TModel>> responses;
  private @Nullable BongoFindResponse<TModel> nextResponse;
  public static final BongoFindResponse<?> END_RESPONSE = new BongoFindResponse<>();
  public static final BongoFindResponse<?> ABORT_RESPONSE = new BongoFindResponse<>();
  private @Nullable ReadExecutionStrategy<TModel> strategy;
  private boolean complete;

  private @Nullable Throwable triggeringException;

  public BongoDbBatchBlockingCursor(
      BlockingQueue<BongoFindResponse<TModel>> responses, ReadExecutionStrategy<TModel> strategy) {
    this.responses = responses;
    this.strategy = strategy;
  }

  public void setTriggeringException(@Nullable Throwable triggeringException) {
    this.triggeringException = triggeringException;
  }

  @Override
  public boolean hasNext() {
    if (this.nextResponse != null) {
      return true;
    }

    log.debug("hasNext in blocking cursor, next is null blocking on queue");
    BongoFindResponse<TModel> next = null;
    try {
      next = responses.take();
    } catch (InterruptedException e) {
      throw new BongoException(e);
    }

    if (next == ABORT_RESPONSE) {
      var localTriggeringException = triggeringException;
      if (localTriggeringException != null) {
        throw new BongoException(localTriggeringException);
      }

      throw new BongoException("Abort response placed on queue, but no trigger exception provided");
    }

    if (next == END_RESPONSE) {
      log.debug("hasNext in blocking cursor, next == endResponse");
      complete = true;
      return false;
    }
    if (next.getBatch() == null) {
      throw new BongoException("batch should never be null");
    }
    log.debug(
        "hasNext in blocking cursor, retrieved batch size {} from queue", next.getBatch().size());
    this.nextResponse = next;
    return true;
  }

  @Override
  public List<TModel> next() {
    if (nextResponse != null) {
      var nr = nextResponse.getBatch();
      nextResponse = null;
      return nr;
    }

    if (hasNext()) {
      if (nextResponse != null) {
        var nr = nextResponse.getBatch();
        nextResponse = null;
        return nr;
      }
    }

    throw new IllegalStateException("iterator exhausted");
  }

  public void close() {
    if (strategy != null) {
      strategy.close();
    }
    strategy = null;
  }
}
