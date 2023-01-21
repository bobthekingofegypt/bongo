package org.bobstuff.bongo.exception;

import java.io.Serial;
import java.util.List;
import org.bobstuff.bongo.messages.BongoBulkWriteError;

public class BongoBulkWriteException extends BongoException {
  @Serial private static final long serialVersionUID = 1L;
  private List<BongoBulkWriteError> writeErrors;

  public BongoBulkWriteException(List<BongoBulkWriteError> writeErrors) {
    super("Bulk write operation resulted in errors");
    this.writeErrors = writeErrors;
  }

  public List<BongoBulkWriteError> getWriteErrors() {
    return writeErrors;
  }
}
