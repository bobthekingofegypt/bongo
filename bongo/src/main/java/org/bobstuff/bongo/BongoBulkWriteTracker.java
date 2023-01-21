package org.bobstuff.bongo;

import java.util.ArrayList;
import java.util.List;
import org.bobstuff.bongo.messages.BongoBulkWriteError;
import org.bobstuff.bongo.messages.BongoBulkWriteResponse;

public class BongoBulkWriteTracker {
  private int numberCompleted;
  private List<BongoBulkWriteError> writeErrors;

  private boolean ordered;

  public BongoBulkWriteTracker(boolean ordered) {
    writeErrors = new ArrayList<>();
    this.ordered = ordered;
  }

  public void addResponse(BongoBulkWriteResponse response) {
    numberCompleted += response.getN();
    if (response.getWriteErrors() != null) {
      writeErrors.addAll(response.getWriteErrors());
    }
  }

  public void mergeTracker(BongoBulkWriteTracker tracker) {
    numberCompleted += tracker.getNumberCompleted();
    writeErrors.addAll(tracker.getWriteErrors());
  }

  public boolean shouldAbort() {
    return ordered && writeErrors.size() > 0;
  }

  public boolean hasErrors() {
    return writeErrors.size() > 0;
  }

  public int getNumberCompleted() {
    return numberCompleted;
  }

  public void setNumberCompleted(int numberCompleted) {
    this.numberCompleted = numberCompleted;
  }

  public List<BongoBulkWriteError> getWriteErrors() {
    return writeErrors;
  }
}
