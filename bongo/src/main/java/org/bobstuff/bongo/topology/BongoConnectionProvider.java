package org.bobstuff.bongo.topology;

import org.bobstuff.bongo.connection.BongoSocket;

public interface BongoConnectionProvider {
  BongoSocket getReadConnection();

  BongoSocket getReadConnection(ServerAddress serverAddress);
}
