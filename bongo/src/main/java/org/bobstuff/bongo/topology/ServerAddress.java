package org.bobstuff.bongo.topology;

import java.net.URI;
import lombok.Data;

@Data
public class ServerAddress {
  private final String host;
  private final int port;

  public ServerAddress(String address) {
    URI uri = URI.create("scheme://" + address);
    this.host = uri.getHost();
    this.port = uri.getPort();
  }

  public ServerAddress(String host, int port) {
    // TODO validate host and port
    this.host = host;
    this.port = port;
  }
}
