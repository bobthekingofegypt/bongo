package org.bobstuff.bongo;

import java.util.Map;
import lombok.ToString;

@ToString
public class BongoInsertManyResultUnacknowledged implements BongoInsertManyResult {
  public BongoInsertManyResultUnacknowledged() {}

  @Override
  public Map<Integer, byte[]> getInsertedIds() {
    throw new UnsupportedOperationException(
        "Inserted IDs are not provided for unacknowledged insert many requests");
  }

  @Override
  public boolean isAcknowledged() {
    return false;
  }
}
