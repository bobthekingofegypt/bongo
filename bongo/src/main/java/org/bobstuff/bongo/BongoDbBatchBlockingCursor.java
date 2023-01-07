package org.bobstuff.bongo;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class BongoDbBatchBlockingCursor<TModel> implements BongoDbBatchCursor<TModel> {
  private BlockingQueue<BongoFindResponse<TModel>> responses;
  private @Nullable BongoFindResponse<TModel> nextResponse;
  public static final BongoFindResponse<?> END_RESPONSE = new BongoFindResponse<>();

  public BongoDbBatchBlockingCursor(BlockingQueue<BongoFindResponse<TModel>> responses) {
    this.responses = responses;
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
      throw new RuntimeException(e);
    }

    if (next == END_RESPONSE) {
      log.debug("hasNext in blocking cursor, next == endResponse");
      return false;
    }
    log.debug(
        "hasNext in blocking cursor, retreived batch size {} from queue", next.getBatch().size());
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
}
