package org.bobstuff.bongo.exception;

import java.io.Serial;

public class BongoSdamException extends BongoConnectionException {
  @Serial private static final long serialVersionUID = 1L;

  public BongoSdamException(String message) {
    super(message);
  }

  public BongoSdamException(String message, Throwable cause) {
    super(message, cause);
  }

  public BongoSdamException(Throwable cause) {
    super(cause);
  }

  public BongoSdamException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
