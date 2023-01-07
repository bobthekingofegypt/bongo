package org.bobstuff.bongo.topology;

import org.bobstuff.bongo.connection.BongoSocket;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface BongoConnectionProvider {
  BongoSocket getReadConnection();

  @NonNull
  BongoSocket getReadConnection(ServerAddress serverAddress);
}
