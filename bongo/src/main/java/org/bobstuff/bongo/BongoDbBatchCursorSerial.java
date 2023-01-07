package org.bobstuff.bongo;

import java.util.List;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoDbBatchCursorSerial<TModel> implements BongoDbBatchCursor<TModel> {
  private BongoFindResponse<TModel> firstResponse;
  private @Nullable List<TModel> batch;

  private Class<TModel> model;
  private BongoDbBatchCursorGetMore<TModel> getMoreExecutor;

  public BongoDbBatchCursorSerial(
      BongoFindResponse<TModel> firstResponse,
      Class<TModel> model,
      BongoDbBatchCursorGetMore<TModel> getMore) {
    this.model = model;
    this.firstResponse = firstResponse;
    this.getMoreExecutor = getMore;
    this.batch = firstResponse.getBatch();
  }

  @Override
  public boolean hasNext() {
    if (batch != null && batch.size() > 0) {
      return true;
    }

    getMore();

    if (batch != null && batch.size() > 0) {
      return true;
    }

    return false;
  }

  @Override
  public List<TModel> next() {
    if (hasNext()) {
      var nextBatch = batch;
      if (nextBatch == null) {
        throw new RuntimeException("TODO change this");
      }
      this.batch = null;
      return nextBatch;
    }

    throw new IllegalStateException("iterator exhausted");
  }

  private void getMore() {
    var result = getMoreExecutor.getMore();
    if (result == null) {
      return;
    }
    if (result.getOk() == 1.0) {
      batch = result.getBatch();
    } else {
      throw new RuntimeException(result.getErrmsg());
    }
  }
}
