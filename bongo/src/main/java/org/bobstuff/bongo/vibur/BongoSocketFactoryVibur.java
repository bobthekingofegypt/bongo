package org.bobstuff.bongo.vibur;

import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticator;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.connection.BongoSocketInitialiser;
import org.bobstuff.bongo.topology.ServerAddress;
import org.vibur.objectpool.PoolObjectFactory;

@Slf4j
public class BongoSocketFactoryVibur implements PoolObjectFactory<BongoSocket> {
  private final ServerAddress serverAddress;
  private final BongoAuthenticator authenticator;
  private final BufferDataPool bufferPool;

  private final WireProtocol wireProtocol;
  private BongoSocketInitialiser socketInitialiser;

  public BongoSocketFactoryVibur(
      ServerAddress serverAddress,
      BongoSocketInitialiser socketInitialiser,
      BongoAuthenticator authenticator,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool) {
    this.serverAddress = serverAddress;
    this.authenticator = authenticator;
    this.socketInitialiser = socketInitialiser;
    this.wireProtocol = wireProtocol;
    this.bufferPool = bufferPool;
  }

  @Override
  public BongoSocket create() {
    log.trace("Creating new socket connection for address {}", serverAddress);
    var bongoSocket =
        new BongoSocket(serverAddress, socketInitialiser, authenticator, wireProtocol, bufferPool);
    bongoSocket.open();

    return bongoSocket;
  }

  @Override
  public void destroy(BongoSocket bongoSocket) {
    bongoSocket.close();
  }

  @Override
  public boolean readyToTake(BongoSocket obj) {
    return true;
  }

  @Override
  public boolean readyToRestore(BongoSocket obj) {
    return true;
  }
}
