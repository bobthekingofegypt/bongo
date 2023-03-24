package org.bobstuff.bongo;

import lombok.ToString;

import java.util.Map;

@ToString
public class BongoBulkWriteResultAcknowledged implements BongoBulkWriteResult {
  private final Map<Integer, byte[]> insertedIds;
  private BongoBulkWriteTracker tracker;

  public BongoBulkWriteResultAcknowledged(Map<Integer, byte[]> insertedIds, BongoBulkWriteTracker tracker) {
    this.insertedIds = insertedIds;
    this.tracker = tracker;
  }

  @Override
  public int getDeletedCount() {
    return tracker.getDeletedCount();
  }

  @Override
  public Map<Integer, byte[]> getInsertedIds() {
    return insertedIds;
  }

  @Override
  public boolean isAcknowledged() {
    return true;
  }

  @Override
  public int getInsertedCount() {
    return insertedIds.size();
  }
}
