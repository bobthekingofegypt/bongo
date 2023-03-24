package org.bobstuff.bongo;

import java.util.Map;

public interface BongoInsertManyResult {
  Map<Integer, byte[]> getInsertedIds();

  int getDeletedCount();
  boolean isAcknowledged();
}
