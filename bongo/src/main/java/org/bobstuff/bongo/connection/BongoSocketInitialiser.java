package org.bobstuff.bongo.connection;

import java.util.List;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticator;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.exception.BongoAuthenticatorException;
import org.bobstuff.bongo.topology.HelloResponse;
import org.bobstuff.bongo.topology.ServerDescription;
import org.bson.*;

public class BongoSocketInitialiser {
  private BongoCodec codec;
  //    private BufferDataPool bufferPool;
  private WireProtocol wireProtocol;
  private BongoAuthenticator authenticator;
  private List<BongoCompressor> compressors;

  public BongoSocketInitialiser(
      BongoCodec codec,
      WireProtocol wireProtocol,
      BongoAuthenticator authenticator,
      List<BongoCompressor> compressors) {
    this.codec = codec;
    this.wireProtocol = wireProtocol;
    this.compressors = compressors;
    this.authenticator = authenticator;
  }

  public BongoSocketInitialiserResult initialise(BongoSocket socket) {
    BsonArray compressorsArray = new BsonArray(compressors.size());
    for (var compressor : compressors) {
      compressorsArray.add(new BsonString(compressor.getIdentifier()));
    }

    BsonDocument command =
        new BsonDocument()
            .append("hello", new BsonInt32(1))
            .append("helloOk", new BsonBoolean(true))
            .append("compression", compressorsArray)
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

    authenticator.authenticate(socket, wireProtocol, initialResponse);

    var serverDescription = ServerDescription.from(socket.getServerAddress(), initialResponse);

    // TODO read compressors from the response
    System.out.println(initialResponse.getCompression());

    return new BongoSocketInitialiserResult(serverDescription, compressors.get(0));
  }
}
