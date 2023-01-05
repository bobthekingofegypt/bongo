package org.bobstuff.bongo.auth;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BongoCredentialsTest {
  @Test
  public void testCreation() {
    var sut =
        BongoCredentials.builder().username("bob").password("pass").authSource("admin").build();
    Assertions.assertEquals("bob", sut.getUsername());
    Assertions.assertEquals("pass", sut.getPassword());
  }

  @Test
  public void testBothUsernameAndPassMustBeSet() {
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              BongoCredentials.builder().username("bob").build();
            },
            "IllegalArgumentException was expected");

    Assertions.assertEquals("username, password and authSource must be set", thrown.getMessage());
  }

  @Test
  public void testBothUsernameAndPassMustBeSetFlipped() {
    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> BongoCredentials.builder().password("bob").build(),
            "IllegalArgumentException was expected");

    Assertions.assertEquals("username, password and authSource must be set", thrown.getMessage());
  }
}
