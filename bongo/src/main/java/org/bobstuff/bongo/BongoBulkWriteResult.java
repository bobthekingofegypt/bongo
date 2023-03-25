package org.bobstuff.bongo;

import java.util.List;
import java.util.Map;
import org.bobstuff.bongo.messages.BongoIndexedIdOperation;

public interface BongoBulkWriteResult {
  Map<Integer, byte[]> getInsertedIds();

  int getDeletedCount();

  boolean isAcknowledged();

  int getInsertedCount();

  int getMatchedCount();

  int getModifiedCount();

  List<BongoIndexedIdOperation> getUpsertedIds();
}
