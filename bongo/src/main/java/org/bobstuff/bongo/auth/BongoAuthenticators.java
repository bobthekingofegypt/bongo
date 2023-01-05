package org.bobstuff.bongo.auth;

import org.bobstuff.bongo.BongoSettings;

public class BongoAuthenticators {
  public static BongoAuthenticator from(BongoSettings settings) {
    if (settings.getConnectionSettings().getCredentials() != null) {
      return new BongoAuthenticatorScram(
          settings.getConnectionSettings().getCredentials(),
          settings.getBufferPool(),
          settings.getCodec());
    }

    return new NoopAuthenticator(settings.getCodec(), settings.getBufferPool());
  }
}
