package org.bobstuff.bongo;

public class BongoDeleteResultAcknowledged implements BongoDeleteResult {
  private BongoBulkWriteResult result;

  public BongoDeleteResultAcknowledged(BongoBulkWriteResult result) {
    this.result = result;
  }

  @Override
  public int getDeletedCount() {
    return result.getDeletedCount();
  }

  @Override
  public boolean isAcknowledged() {
    return true;
  }
}
