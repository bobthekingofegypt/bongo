package org.bobstuff.bongo.auth;

import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.topology.HelloResponse;

public interface BongoAuthenticator {
  HelloResponse authenticate(BongoSocket socket, WireProtocol wireProtocol);
}
