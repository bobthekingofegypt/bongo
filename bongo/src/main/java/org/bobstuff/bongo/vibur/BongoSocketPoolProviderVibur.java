package org.bobstuff.bongo.vibur;

import lombok.NonNull;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticator;
import org.bobstuff.bongo.connection.BongoSocketInitialiser;
import org.bobstuff.bongo.connection.BongoSocketPool;
import org.bobstuff.bongo.connection.BongoSocketPoolProvider;
import org.bobstuff.bongo.topology.ServerAddress;

public class BongoSocketPoolProviderVibur implements BongoSocketPoolProvider {
  private int maxSize;

  public BongoSocketPoolProviderVibur() {
    this(60);
  }

  public BongoSocketPoolProviderVibur(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public @NonNull BongoSocketPool provide(
      ServerAddress serverAddress,
      BongoSocketInitialiser socketInitialiser,
      BongoAuthenticator authenticator,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool) {
    BongoSocketFactoryVibur socketFactory =
        new BongoSocketFactoryVibur(
            serverAddress, socketInitialiser, authenticator, wireProtocol, bufferPool);
    return new BongoSocketPoolVibur(socketFactory, 0, maxSize);
  }
}
