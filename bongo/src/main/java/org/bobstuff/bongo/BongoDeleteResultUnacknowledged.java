package org.bobstuff.bongo;

import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoDeleteResultUnacknowledged implements BongoDeleteResult {
  @Override
  public int getDeletedCount() {
    throw new UnsupportedOperationException(
        "Deleted count is not supported in unacknowledged requests");
  }

  @Override
  public boolean isAcknowledged() {
    return false;
  }
}
