package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoInsertOneResultAcknowledged implements BongoInsertOneResult {
  private byte @Nullable [] id;

  public BongoInsertOneResultAcknowledged(byte @Nullable [] id) {
    this.id = id;
  }

  @Override
  public boolean wasAcknowledged() {
    return true;
  }

  @Override
  public byte @Nullable [] getInsertedId() {
    return id;
  }
}
