package org.bobstuff.bongo.executionstrategy;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BufferDataPool;
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
  @Override
  public BongoDbBatchCursor<TModel> execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      BongoFindOptions findOptions,
      BsonDocument filter,
      @Nullable Boolean compress,
      BongoCursorType cursorType,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BufferDataPool bufferPool,
      BongoConnectionProvider connectionProvider) {
    // TODO can we avoid these allocations
    var findRequestConverter = new BongoFindRequestConverter(codec.converter(BsonDocument.class));
    var findResponseConverter =
        new BongoFindResponseConverter<TModel>(codec.converter(model), false);

    var socket = connectionProvider.getReadConnection();
    var serverAddress = socket.getServerAddress();
    var compressor = socket.getCompressor();
    if (compressor == null && compress != null && compress) {
      throw new IllegalStateException(
          "Compression requested on call but no compressors registered");
    }
    var requestCompression = compressor != null && (compress == null || compress);

    var response =
        wireProtocol.sendReceiveCommandMessage(
            socket,
            findRequestConverter,
            new BongoFindRequest(identifier, findOptions, filter),
            findResponseConverter,
            requestCompression,
            false);

    socket.release();

    var getMore =
        new ExecuteGetMore<TModel>(
            socket,
            identifier,
            serverAddress,
            response,
            requestCompression,
            cursorType,
            wireProtocol,
            findOptions,
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
    private boolean compress;
    private BongoCursorType cursorType;

    public ExecuteGetMore(
        BongoSocket socket,
        BongoCollection.Identifier identifier,
        ServerAddress serverAddress,
        WireProtocol.Response<BongoFindResponse<TModel>> lastResponse,
        boolean compress,
        BongoCursorType cursorType,
        WireProtocol wireProtocol,
        @Nullable BongoFindOptions findOptions,
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
      this.compress = compress;
      this.cursorType = cursorType;
      this.getMoreRequest =
          new BongoGetMoreRequest(
              lastResponse.getPayload().getId(),
              identifier.getDatabaseName(),
              identifier.getCollectionName(),
              findOptions != null ? findOptions.getBatchSize() : null);
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

      var localSocket = socket;
      if (localSocket == null) {
        localSocket = connectionProvider.getReadConnection(serverAddress);

        wireProtocol.sendCommandMessage(
            localSocket,
            requestConverter,
            getMoreRequest,
            compress,
            cursorType == BongoCursorType.Exhaustible);
      }

      var response = wireProtocol.readServerResponse(localSocket, responseConverter);
      lastResponse = response;

      if (cursorType != BongoCursorType.Exhaustible) {
        localSocket.release();
        socket = null;
      } else {
        socket = localSocket;
      }

      return response.getPayload();
    }

    @Override
    public void abort() {
      var localSocket = socket;
      // exhaustible connections that weren't complete must be either consumed or
      // destroyed, it's easier to just destroy them.
      if (cursorType == BongoCursorType.Exhaustible && localSocket != null) {
        localSocket.close();
        socket = null;
      }
    }
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public boolean isClosed() {
    return false;
  }
}
