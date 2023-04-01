package org.bobstuff.bongo;

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
