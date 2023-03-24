package org.bobstuff.bongo;

import lombok.ToString;

import java.util.Map;

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
    throw new UnsupportedOperationException("Deletion count is not provided for unacknowledged bulk requests");

  }

  @Override
  public boolean isAcknowledged() {
    return false;
  }

  @Override
  public int getInsertedCount() {
    throw new UnsupportedOperationException("Inserted count is not provided for unacknowledged bulk requests");
  }


}
