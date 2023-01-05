package org.bobstuff.bongo.vibur;

import lombok.NonNull;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticator;
import org.bobstuff.bongo.connection.BongoSocketPool;
import org.bobstuff.bongo.connection.BongoSocketPoolProvider;
import org.bobstuff.bongo.topology.ServerAddress;

public class BongoSocketPoolProviderVibur implements BongoSocketPoolProvider {
  @Override
  public @NonNull BongoSocketPool provide(
      ServerAddress serverAddress,
      BongoAuthenticator authenticator,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool) {
    BongoSocketFactoryVibur socketFactory =
        new BongoSocketFactoryVibur(serverAddress, authenticator, wireProtocol, bufferPool);
    return new BongoSocketPoolVibur(socketFactory, 0, 60);
  }
}
