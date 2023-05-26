package org.bobstuff.bongo.connection;

import lombok.NonNull;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticator;
import org.bobstuff.bongo.topology.ServerAddress;

@FunctionalInterface
public interface BongoSocketPoolProvider {
  @NonNull
  BongoSocketPool provide(
      ServerAddress serverAddress,
      BongoSocketInitialiser socketInitialiser,
      BongoAuthenticator authenticator,
      WireProtocol wireProtocol,
      BobBsonBufferPool bufferPool);
}
