package org.bobstuff.bongo.exception;

import java.io.Serial;

public class BongoSocketWriteException extends BongoConnectionException {
  @Serial private static final long serialVersionUID = 1L;

  public BongoSocketWriteException(String message) {
    super(message);
  }

  public BongoSocketWriteException(String message, Throwable cause) {
    super(message, cause);
  }

  public BongoSocketWriteException(Throwable cause) {
    super(cause);
  }

  public BongoSocketWriteException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
