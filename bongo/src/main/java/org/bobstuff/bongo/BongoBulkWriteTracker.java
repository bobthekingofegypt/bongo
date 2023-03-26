package org.bobstuff.bongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bobstuff.bongo.messages.BongoBulkWriteError;
import org.bobstuff.bongo.messages.BongoBulkWriteResponse;
import org.bobstuff.bongo.messages.BongoIndexedIdOperation;

public class BongoBulkWriteTracker {
  private int deletedCount;
  private List<BongoBulkWriteError> writeErrors;

  private Map<Integer, byte[]> insertedIds;
  private List<BongoIndexedIdOperation> upsertedIds;
  private int matchedCount;
  private int modifiedCount;

  private int insertedCount;
  private boolean ordered;

  public BongoBulkWriteTracker(boolean ordered) {
    writeErrors = new ArrayList<>();
    insertedIds = new HashMap<>();
    upsertedIds = new ArrayList<>();
    this.ordered = ordered;
  }

  public BongoBulkWriteTracker(boolean ordered, Map<Integer, byte[]> insertedIds) {
    writeErrors = new ArrayList<>();
    this.insertedIds = insertedIds;
    upsertedIds = new ArrayList<>();
    this.ordered = ordered;
  }

  public void addResponse(
      BongoWriteOperationType operationType,
      BongoBulkWriteResponse response,
      Map<Integer, byte[]> insertedIds,
      BongoIndexMap indexMap) {
    if (operationType == BongoWriteOperationType.Delete) {
      deletedCount += response.getN();
    }

    if (response.getWriteErrors() != null) {
      writeErrors.addAll(response.getWriteErrors());
    }

    if (operationType == BongoWriteOperationType.Update) {
      matchedCount += response.getN();
      modifiedCount += response.getNModified();
      if (response.getUpserted() != null) {
        for (var upsert : response.getUpserted()) {
          upsert.setIndex(indexMap.get(upsert.getIndex()));
          upsertedIds.add(upsert);
        }
      }
    }

    if (operationType == BongoWriteOperationType.Insert) {
      insertedCount += response.getN();
    }

    //    this.insertedIds.putAll(insertedIds);
  }

  public void mergeTracker(BongoBulkWriteTracker tracker) {
    writeErrors.addAll(tracker.getWriteErrors());
    matchedCount += tracker.getMatchedCount();
    modifiedCount += tracker.getModifiedCount();
    upsertedIds.addAll(tracker.getUpsertedIds());
    deletedCount += tracker.getDeletedCount();
    insertedCount += tracker.getInsertedCount();
  }

  public boolean shouldAbort() {
    return ordered && writeErrors.size() > 0;
  }

  public boolean hasErrors() {
    return writeErrors.size() > 0;
  }

  public int getDeletedCount() {
    return deletedCount;
  }

  public List<BongoBulkWriteError> getWriteErrors() {
    return writeErrors;
  }

  public Map<Integer, byte[]> getInsertedIds() {
    return insertedIds;
  }

  public List<BongoIndexedIdOperation> getUpsertedIds() {
    return upsertedIds;
  }

  public int getMatchedCount() {
    return matchedCount;
  }

  public int getModifiedCount() {
    return modifiedCount;
  }

  public int getInsertedCount() {
    return insertedCount;
  }
}
