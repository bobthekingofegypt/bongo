package org.bobstuff.bongo.auth;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class BongoCredentials {
  private final String username;
  private final String password;
  private final String authSource;

  public static class BongoCredentialsBuilder {
    public BongoCredentials build() {
      if (username == null || password == null || authSource == null) {
        throw new IllegalArgumentException("username, password and authSource must be set");
      }

      return new BongoCredentials(username, password, authSource);
    }
  }
}
