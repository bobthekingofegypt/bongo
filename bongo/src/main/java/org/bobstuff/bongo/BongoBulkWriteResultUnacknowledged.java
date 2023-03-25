package org.bobstuff.bongo;

import java.util.List;
import java.util.Map;
import lombok.ToString;
import org.bobstuff.bongo.messages.BongoIndexedIdOperation;

@ToString
public class BongoBulkWriteResultUnacknowledged implements BongoBulkWriteResult {
  public BongoBulkWriteResultUnacknowledged() {}

  @Override
  public Map<Integer, byte[]> getInsertedIds() {
    throw new UnsupportedOperationException(
        "Inserted IDs are not provided for unacknowledged insert many requests");
  }

  @Override
  public int getDeletedCount() {
    throw new UnsupportedOperationException(
        "Deletion count is not provided for unacknowledged bulk requests");
  }

  @Override
  public boolean isAcknowledged() {
    return false;
  }

  @Override
  public int getInsertedCount() {
    throw new UnsupportedOperationException(
        "Inserted count is not provided for unacknowledged bulk requests");
  }

  @Override
  public int getMatchedCount() {
    throw new UnsupportedOperationException(
        "Matched count is not provided for unacknowledged bulk requests");
  }

  @Override
  public int getModifiedCount() {
    throw new UnsupportedOperationException(
        "Modified count is not provided for unacknowledged bulk requests");
  }

  @Override
  public List<BongoIndexedIdOperation> getUpsertedIds() {
    throw new UnsupportedOperationException(
        "Upserted IDs are not provided for unacknowledged bulk requests");
  }
}
