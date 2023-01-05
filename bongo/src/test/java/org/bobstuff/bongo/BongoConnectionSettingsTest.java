package org.bobstuff.bongo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BongoConnectionSettingsTest {

  @Test
  public void testConnectionSettingsBuilder() {
    var sut =
        BongoConnectionSettings.builder()
            .host("localhost:27017")
            .host("localhost:27018")
            .host("localhost:27019")
            .build();

    Assertions.assertEquals(3, sut.getHosts().size());
    Assertions.assertEquals("localhost:27017", sut.getHosts().get(0));
    Assertions.assertEquals("localhost:27018", sut.getHosts().get(1));
    Assertions.assertEquals("localhost:27019", sut.getHosts().get(2));
  }
}
