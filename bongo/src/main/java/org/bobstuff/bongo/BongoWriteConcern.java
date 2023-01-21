package org.bobstuff.bongo;

import lombok.Value;
import org.checkerframework.checker.nullness.qual.Nullable;

@Value
public class BongoWriteConcern {
  private final @Nullable String wString;
  private final int wNumber;
  private @Nullable final Boolean journal;

  public BongoWriteConcern(String wString) {
    this.wString = wString;
    this.journal = null;
    this.wNumber = -1;
  }

  public BongoWriteConcern(int wNumber) {
    this.wString = null;
    this.journal = null;
    this.wNumber = wNumber;
  }

  public BongoWriteConcern(String wString, boolean journal) {
    this.wString = wString;
    this.wNumber = -1;
    this.journal = journal;
  }

  public BongoWriteConcern(int wNumber, boolean journal) {
    this.wString = null;
    this.wNumber = wNumber;
    this.journal = journal;
  }

  public boolean isAcknowledged() {
    if (wNumber != -1) {
      return wNumber > 0 || (journal != null && journal);
    }
    return true;
  }
}
