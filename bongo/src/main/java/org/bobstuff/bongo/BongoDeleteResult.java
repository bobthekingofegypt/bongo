package org.bobstuff.bongo;

public interface BongoDeleteResult {
  int getDeletedCount();

  boolean isAcknowledged();
}
