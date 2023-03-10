package org.bobstuff.bongo;

public enum BongoWriteOperationType {
  Update("update", "updates"),
  Insert("insert", "documents");

  private String command;
  private String payload;

  private BongoWriteOperationType(String command, String payload) {
    this.command = command;
    this.payload = payload;
  }

  public String getCommand() {
    return command;
  }

  public String getPayload() {
    return payload;
  }
}
