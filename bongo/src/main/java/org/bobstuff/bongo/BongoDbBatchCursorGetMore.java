package org.bobstuff.bongo;

import org.bobstuff.bongo.messages.BongoFindResponse;
import org.checkerframework.checker.nullness.qual.Nullable;

// @FunctionalInterface
public interface BongoDbBatchCursorGetMore<TModel> {
  @Nullable BongoFindResponse<TModel> getMore();

  void abort();
}
