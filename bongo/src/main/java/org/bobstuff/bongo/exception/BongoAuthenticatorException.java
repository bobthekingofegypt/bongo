package org.bobstuff.bongo.exception;

import java.io.Serial;

public class BongoAuthenticatorException extends BongoConnectionException {
  @Serial private static final long serialVersionUID = 1L;

  public BongoAuthenticatorException(String message) {
    super(message);
  }

  public BongoAuthenticatorException(String message, Throwable cause) {
    super(message, cause);
  }

  public BongoAuthenticatorException(Throwable cause) {
    super(cause);
  }

  public BongoAuthenticatorException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
