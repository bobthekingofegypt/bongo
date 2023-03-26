package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoDeleteResultAcknowledged implements BongoDeleteResult {
  private BongoBulkWriteResult result;

  public BongoDeleteResultAcknowledged(BongoBulkWriteResult result) {
    this.result = result;
  }

  @Override
  public int getDeletedCount() {
    return result.getDeletedCount();
  }

  @Override
  public boolean isAcknowledged() {
    return true;
  }
}
