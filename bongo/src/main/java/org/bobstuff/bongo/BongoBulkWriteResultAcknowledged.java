package org.bobstuff.bongo;

import java.util.List;
import java.util.Map;
import lombok.ToString;
import org.bobstuff.bongo.messages.BongoIndexedIdOperation;

@ToString
public class BongoBulkWriteResultAcknowledged implements BongoBulkWriteResult {
  private final Map<Integer, byte[]> insertedIds;
  private BongoBulkWriteTracker tracker;

  public BongoBulkWriteResultAcknowledged(
      Map<Integer, byte[]> insertedIds, BongoBulkWriteTracker tracker) {
    this.insertedIds = insertedIds;
    this.tracker = tracker;
  }

  @Override
  public int getDeletedCount() {
    return tracker.getDeletedCount();
  }

  @Override
  public Map<Integer, byte[]> getInsertedIds() {
    return tracker.getInsertedIds();
  }

  @Override
  public boolean isAcknowledged() {
    return true;
  }

  @Override
  public int getInsertedCount() {
    return tracker.getInsertedIds().size();
  }

  @Override
  public int getMatchedCount() {
    return tracker.getMatchedCount();
  }

  @Override
  public int getModifiedCount() {
    return tracker.getModifiedCount();
  }

  @Override
  public List<BongoIndexedIdOperation> getUpsertedIds() {
    return tracker.getUpsertedIds();
  }
}
