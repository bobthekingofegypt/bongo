package org.bobstuff.bongo;

import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.converters.BongoCountRequestConverter;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.exception.BongoReadException;
import org.bobstuff.bongo.messages.BongoCountResponse;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;

public class BongoCountExecutor {
  public long execute(
      BongoCollection.Identifier identifier,
      BongoFindOptions findOptions,
      BsonDocument filter,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider) {
    var countRequestConverter = new BongoCountRequestConverter(codec.converter(BsonDocument.class));
    var socket = connectionProvider.getReadConnection();

    var response =
        wireProtocol.sendReceiveCommandMessage(
            socket,
            countRequestConverter,
            new BongoFindRequest(identifier, findOptions, filter, null),
            codec.converter(BongoCountResponse.class),
            false,
            false);

    var payload = response.payload();
    if (payload != null) {
      if (payload.isOk()) {
        return payload.getN();
      }
      throw new BongoReadException(payload.getCode(), payload.getErrmsg(), payload.getCodeName());
    }

    throw new BongoException("Failed to read payload from count request");
  }
}
