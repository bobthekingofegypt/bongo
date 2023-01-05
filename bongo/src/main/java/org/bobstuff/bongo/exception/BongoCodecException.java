package org.bobstuff.bongo.exception;

import java.io.Serial;

public class BongoCodecException extends BongoConnectionException {
  @Serial private static final long serialVersionUID = 1L;

  public BongoCodecException(String message) {
    super(message);
  }

  public BongoCodecException(String message, Throwable cause) {
    super(message, cause);
  }

  public BongoCodecException(Throwable cause) {
    super(cause);
  }

  public BongoCodecException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
