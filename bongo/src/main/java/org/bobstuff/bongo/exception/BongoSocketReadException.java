package org.bobstuff.bongo.exception;

import java.io.Serial;

public class BongoSocketReadException extends BongoConnectionException {
  @Serial private static final long serialVersionUID = 1L;

  public BongoSocketReadException(String message) {
    super(message);
  }

  public BongoSocketReadException(String message, Throwable cause) {
    super(message, cause);
  }

  public BongoSocketReadException(Throwable cause) {
    super(cause);
  }

  public BongoSocketReadException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
