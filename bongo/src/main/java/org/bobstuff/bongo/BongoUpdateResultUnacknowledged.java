package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoUpdateResultUnacknowledged implements BongoUpdateResult {
  @Override
  public int getMatchedCount() {
    throw new UnsupportedOperationException(
        "Matched count is not supported in unacknowledged requests");
  }

  @Override
  public int getModifiedCount() {
    throw new UnsupportedOperationException(
        "Modified count is not supported in unacknowledged requests");
  }

  @Override
  public byte @Nullable [] getUpsertedId() {
    throw new UnsupportedOperationException(
        "upserted id is not supported in unacknowledged requests");
  }

  @Override
  public boolean isAcknowledged() {
    return false;
  }
}
