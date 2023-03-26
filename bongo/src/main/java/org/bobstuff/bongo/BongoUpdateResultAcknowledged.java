package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoUpdateResultAcknowledged implements BongoUpdateResult {
  private BongoBulkWriteResult result;

  public BongoUpdateResultAcknowledged(BongoBulkWriteResult result) {
    this.result = result;
  }

  @Override
  public int getMatchedCount() {
    return result.getMatchedCount();
  }

  @Override
  public int getModifiedCount() {
    return result.getModifiedCount();
  }

  @Override
  public byte @Nullable [] getUpsertedId() {
    if (result.getUpsertedIds() != null && result.getUpsertedIds().size() > 0) {
      return result.getUpsertedIds().get(0).getObjectId();
    }
    return null;
  }

  @Override
  public boolean isAcknowledged() {
    return true;
  }
}
