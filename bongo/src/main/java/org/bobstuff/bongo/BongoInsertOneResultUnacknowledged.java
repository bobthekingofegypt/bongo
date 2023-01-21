package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoInsertOneResultUnacknowledged implements BongoInsertOneResult {
  @Override
  public boolean wasAcknowledged() {
    return false;
  }

  @Override
  public byte @Nullable [] getInsertedId() {
    throw new UnsupportedOperationException("Inserted ID not available in unacknowledged writes");
  }
}
