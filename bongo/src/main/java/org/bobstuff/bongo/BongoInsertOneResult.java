package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface BongoInsertOneResult {
  boolean wasAcknowledged();

  byte @Nullable [] getInsertedId();
}
