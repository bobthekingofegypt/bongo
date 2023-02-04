package org.bobstuff.bongo;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class BongoDbBatchCursorList<TModel> implements BongoDbBatchCursor<TModel> {
  private @Nullable List<TModel> batch;

  public BongoDbBatchCursorList(List<TModel> batch) {
    this.batch = batch;
  }

  @Override
  public boolean hasNext() {
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

  public void close() {
    // no-op
  }
}
