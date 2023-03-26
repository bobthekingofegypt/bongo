package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface BongoUpdateResult {
  int getMatchedCount();

  int getModifiedCount();

  @Nullable byte[] getUpsertedId();

  boolean isAcknowledged();
}
