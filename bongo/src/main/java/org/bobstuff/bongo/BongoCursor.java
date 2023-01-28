package org.bobstuff.bongo;

import java.util.Iterator;
import java.util.List;
import org.bobstuff.bongo.exception.BongoException;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoCursor<TModel> implements Iterator<TModel>, AutoCloseable {
  private BongoDbBatchCursor<TModel> batchCursor;
  private @Nullable List<TModel> data;
  private int index;

  public BongoCursor(BongoDbBatchCursor<TModel> batchCursor) {
    this.batchCursor = batchCursor;
  }

  @Override
  public boolean hasNext() {
    if (data == null || index == data.size()) {
      if (batchCursor.hasNext()) {
        data = batchCursor.next();
        index = 0;
        return true;
      }
    } else if (index < data.size()) {
      return true;
    }
    return false;
  }

  @Override
  public TModel next() {
    if (data != null && index < data.size()) {
      var value = data.get(index);
      index += 1;
      return value;
    }

    if (hasNext()) {
      if (data == null) {
        throw new BongoException("TODO replace this");
      }
      var value = data.get(index);
      index += 1;
      return value;
    }

    throw new IllegalStateException("iterator exhausted");
  }

  public void close() {
    batchCursor.close();
  }
}
