package org.bobstuff.bongo.executionstrategy;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.converters.BongoFindRequestConverter;
import org.bobstuff.bongo.converters.BongoFindResponseConverter;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.bobstuff.bongo.messages.BongoGetMoreRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bobstuff.bongo.topology.ServerAddress;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ReadExecutionSerialStrategy<TModel> implements ReadExecutionStrategy<TModel> {
  private BobBsonConverter<BongoFindRequest> findRequestConverter;
  private BobBsonConverter<BongoFindResponse<TModel>> findResponseConverter;

  public ReadExecutionSerialStrategy(BobBsonConverter<TModel> converter) {
    this.findRequestConverter = new BongoFindRequestConverter();
    this.findResponseConverter = new BongoFindResponseConverter<TModel>(converter, false);
  }

  @Override
  public BongoDbBatchCursor<TModel> execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      @Nullable BongoFindOptions findOptions,
      @Nullable BsonDocument filter,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider) {

    var socket = connectionProvider.getReadConnection();
    var serverAddress = socket.getServerAddress();

    var response =
        wireProtocol.sendReceiveCommandMessage(
            socket,
            findRequestConverter,
            new BongoFindRequest(identifier, findOptions),
            findResponseConverter);

    var getMore =
        new ExecuteGetMore<TModel>(
            socket,
            identifier,
            serverAddress,
            response,
            wireProtocol,
            codec.converter(BongoGetMoreRequest.class),
            findResponseConverter,
            connectionProvider);

    return new BongoDbBatchCursorSerial<>(response.getPayload(), model, getMore);
  }

  private class ExecuteGetMore<TModel> implements BongoDbBatchCursorGetMore<TModel> {
    private @Nullable BongoSocket socket;
    private WireProtocol.Response<BongoFindResponse<TModel>> lastResponse;

    private BongoConnectionProvider connectionProvider;
    private WireProtocol wireProtocol;
    private BobBsonConverter<BongoFindResponse<TModel>> responseConverter;
    private BobBsonConverter<BongoGetMoreRequest> requestConverter;
    private ServerAddress serverAddress;
    private BongoGetMoreRequest getMoreRequest;

    public ExecuteGetMore(
        BongoSocket socket,
        BongoCollection.Identifier identifier,
        ServerAddress serverAddress,
        WireProtocol.Response<BongoFindResponse<TModel>> lastResponse,
        WireProtocol wireProtocol,
        BobBsonConverter<BongoGetMoreRequest> requestConverter,
        BobBsonConverter<BongoFindResponse<TModel>> responseConverter,
        BongoConnectionProvider connectionProvider) {
//      this.socket = socket;
      this.serverAddress = serverAddress;
      this.lastResponse = lastResponse;
      this.wireProtocol = wireProtocol;
      this.responseConverter = responseConverter;
      this.requestConverter = requestConverter;
      this.connectionProvider = connectionProvider;
      this.getMoreRequest =
          new BongoGetMoreRequest(
              lastResponse.getPayload().getId(),
              identifier.getDatabaseName(),
              identifier.getCollectionName());
    }

    @Override
    public @Nullable BongoFindResponse<TModel> getMore() {
      // if cursor id is 0 the cursor is exhausted
      if (lastResponse.getPayload().getId() == 0) {
        if (socket != null) {
          socket.release();
          socket = null;
        }
        return null;
      }

      // HAVENT STARTED STREAMING RESULTS YET
      if (socket == null) {
        socket = connectionProvider.getReadConnection(serverAddress);

        wireProtocol.sendCommandMessage(socket, requestConverter, getMoreRequest, false, true);
      }

      var response = wireProtocol.readServerResponse(socket, responseConverter);
      lastResponse = response;

      return response.getPayload();
    }
  }
}
