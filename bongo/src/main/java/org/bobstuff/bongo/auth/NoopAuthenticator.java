package org.bobstuff.bongo.auth;

import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.*;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoAuthenticatorException;
import org.bobstuff.bongo.topology.HelloResponse;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

@Slf4j
public class NoopAuthenticator implements BongoAuthenticator {
  private BongoCodec codec;
  private BufferDataPool bufferPool;

  public NoopAuthenticator(BongoCodec codec, BufferDataPool bufferPool) {
    this.codec = codec;
    this.bufferPool = bufferPool;
  }

  @Override
  public HelloResponse authenticate(BongoSocket socket, WireProtocol wireProtocol) {
    log.debug("Running the noop authenticator");
    BsonDocument command =
        new BsonDocument()
            .append("hello", new BsonInt32(1))
            .append("helloOk", new BsonBoolean(true))
            .append("$db", new BsonString("admin"));

    var initialResponse =
        wireProtocol
            .sendReceiveCommandMessage(
                socket,
                codec.converter(BsonDocument.class),
                command,
                codec.converter(HelloResponse.class))
            .getPayload();

    if (!initialResponse.isOk()) {
      throw new BongoAuthenticatorException(
          String.format(
              "Failed to authenticate with server: errmsg (%s), code (%s), codeName (%s)",
              initialResponse.getErrmsg(),
              initialResponse.getCode(),
              initialResponse.getCodeName()));
    }

    return initialResponse;
  }
}
