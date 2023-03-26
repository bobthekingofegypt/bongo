package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface BongoDeleteResult {
  int getDeletedCount();
  boolean isAcknowledged();
}
