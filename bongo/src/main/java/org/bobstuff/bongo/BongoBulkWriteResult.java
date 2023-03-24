package org.bobstuff.bongo;

import java.util.Map;

public interface BongoBulkWriteResult {
  Map<Integer, byte[]> getInsertedIds();
  int getDeletedCount();
  boolean isAcknowledged();
  int getInsertedCount();
}
