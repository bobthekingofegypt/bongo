package org.bobstuff.bongo.auth;

import com.bolyartech.scram_sasl.client.ScramClientFunctionality;
import com.bolyartech.scram_sasl.client.ScramClientFunctionalityImpl;
import com.bolyartech.scram_sasl.common.ScramException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoAuthenticatorException;
import org.bobstuff.bongo.topology.HelloResponse;
import org.bson.*;

@Slf4j
public class BongoAuthenticatorScram implements BongoAuthenticator {
  public static final String SHA_256 = "SHA-256";
  public static final String HMAC_SHA_256 = "HmacSHA256";
  public static final String SCRAM_SHA_256 = "SCRAM-SHA-256";
  private BongoCredentials credentials;
  private BufferDataPool bufferPool;

  private BongoCodec codec;

  public BongoAuthenticatorScram(
      BongoCredentials credentials, BufferDataPool bufferPool, BongoCodec codec) {
    this.credentials = credentials;
    this.bufferPool = bufferPool;
    this.codec = codec;
  }

  @Override
  public void authenticate(
      BongoSocket socket, WireProtocol wireProtocol, HelloResponse initialResponse) {
    log.debug(
        "Performing scram authentication for user {} on source {}",
        credentials.getUsername(),
        credentials.getAuthSource());

    var scramClient = new ScramClientFunctionalityImpl(SHA_256, HMAC_SHA_256);
    BsonDocument command =
        new BsonDocument()
            .append("hello", new BsonInt32(1))
            .append("helloOk", new BsonBoolean(true))
            .append("speculativeAuthenticate", createSaslStartCommandDocument(scramClient))
            .append("$db", new BsonString("admin"));

    var initialScramResponse =
        wireProtocol
            .sendReceiveCommandMessage(
                socket,
                codec.converter(BsonDocument.class),
                command,
                codec.converter(HelloResponse.class))
            .getPayload();
    log.trace("Scram initial server response: {}", initialScramResponse);

    if (initialScramResponse.getSpeculativeAuthenticate() == null) {
      throw new BongoAuthenticatorException(
          "No speculative response found on initial server response: " + initialScramResponse);
    }

    var saslContinueCommand =
        new BsonDocument()
            .append("saslContinue", new BsonInt32(1))
            .append(
                "conversationId",
                new BsonInt32(
                    initialScramResponse.getSpeculativeAuthenticate().getConversationId()))
            .append(
                "payload",
                createSaslFirstResponse(
                    scramClient,
                    initialScramResponse.getSpeculativeAuthenticate().getPayload().getData()))
            .append("$db", new BsonString("admin"));

    var finalResponse =
        wireProtocol
            .sendReceiveCommandMessage(
                socket,
                codec.converter(BsonDocument.class),
                saslContinueCommand,
                codec.converter(FinalScramMessage.class))
            .getPayload();

    if (!finalResponse.isOk()) {
      throw new BongoAuthenticatorException(
          String.format(
              "Failed to authenticate with server: errmsg (%s), code (%s), codeName (%s)",
              finalResponse.getErrmsg(), finalResponse.getCode(), finalResponse.getCodeName()));
    }

    log.trace("Scram final server response: {}", finalResponse);

    if (!validateFinalServerResponse(scramClient, finalResponse.getPayload().getData())) {
      throw new BongoAuthenticatorException("Final scram response failed validation");
    }
  }

  private BsonDocument createSaslStartCommandDocument(ScramClientFunctionality scramClient) {
    byte[] payload;
    try {
      String request = scramClient.prepareFirstMessage(credentials.getUsername());
      payload = request.getBytes(StandardCharsets.UTF_8);
    } catch (ScramException e) {
      throw new BongoAuthenticatorException("scram utility failed to generate first message", e);
    }
    return new BsonDocument()
        .append("saslStart", new BsonInt32(1))
        .append("mechanism", new BsonString(SCRAM_SHA_256))
        .append("payload", new BsonBinary(payload))
        .append("db", new BsonString(credentials.getAuthSource()))
        .append("options", new BsonDocument("skipEmptyExchange", new BsonBoolean(true)));
  }

  public BsonBinary createSaslFirstResponse(
      ScramClientFunctionality scramClient, byte[] serverResponse) {
    try {
      String firstResponse =
          scramClient.prepareFinalMessage(credentials.getPassword(), new String(serverResponse));
      return new BsonBinary(firstResponse.getBytes(StandardCharsets.UTF_8));
    } catch (ScramException | NumberFormatException e) {
      throw new BongoAuthenticatorException("Scram utility failed", e);
    }
  }

  public boolean validateFinalServerResponse(ScramClientFunctionality scramClient, byte[] payload) {
    try {
      return scramClient.checkServerFinalMessage(new String(payload));
    } catch (ScramException e) {
      throw new BongoAuthenticatorException("Scram authentication failed", e);
    }
  }
}
