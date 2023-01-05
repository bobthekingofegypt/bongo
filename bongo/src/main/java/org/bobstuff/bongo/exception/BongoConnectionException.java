package org.bobstuff.bongo.exception;

import java.io.Serial;

public class BongoConnectionException extends BongoException {
  @Serial private static final long serialVersionUID = 1L;

  public BongoConnectionException(String message) {
    super(message);
  }

  public BongoConnectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public BongoConnectionException(Throwable cause) {
    super(cause);
  }

  public BongoConnectionException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
