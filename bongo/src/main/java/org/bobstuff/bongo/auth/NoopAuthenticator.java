package org.bobstuff.bongo.auth;

import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.*;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.topology.HelloResponse;
import org.bson.*;

@Slf4j
public class NoopAuthenticator implements BongoAuthenticator {
  private BongoCodec codec;
  private BobBsonBufferPool bufferPool;

  public NoopAuthenticator(BongoCodec codec, BobBsonBufferPool bufferPool) {
    this.codec = codec;
    this.bufferPool = bufferPool;
  }

  @Override
  public void authenticate(
      BongoSocket socket, WireProtocol wireProtocol, HelloResponse initialResponse) {
    log.debug("Running the noop authenticator");
  }
}
