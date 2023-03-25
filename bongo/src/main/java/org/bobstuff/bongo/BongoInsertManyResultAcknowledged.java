package org.bobstuff.bongo;

import java.util.Map;
import lombok.ToString;

@ToString
public class BongoInsertManyResultAcknowledged implements BongoInsertManyResult {
  private final Map<Integer, byte[]> insertedIds;
  public BongoInsertManyResultAcknowledged(
      Map<Integer, byte[]> insertedIds) {
    this.insertedIds = insertedIds;
//    this.tracker = tracker;
  }

//  @Override
//  public int getDeletedCount() {
//    return tracker.getDeletedCount();
//  }

  @Override
  public Map<Integer, byte[]> getInsertedIds() {
    return insertedIds;
  }

  @Override
  public boolean isAcknowledged() {
    return true;
  }
}
