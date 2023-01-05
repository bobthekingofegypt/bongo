package org.bobstuff.bongo.connection;

public enum BongoOpCodes {
  OP_COMPRESSED(2012),
  OP_MSG(2013);

  private int code;

  private BongoOpCodes(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
