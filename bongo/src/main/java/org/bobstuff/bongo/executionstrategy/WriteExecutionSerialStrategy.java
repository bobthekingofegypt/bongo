package org.bobstuff.bongo.executionstrategy;

import java.util.List;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.ContextStack;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.messages.BongoInsertRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WriteExecutionSerialStrategy<TModel> implements WriteExecutionStrategy<TModel> {
  @Override
  public @Nullable BongoInsertManyResult execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      List<TModel> items,
      @Nullable Boolean compress,
      @Nullable BongoInsertProcessor<TModel> insertProcessor,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol) {
    var wrappedItems = new BongoWrappedBulkItems<>(items, codec.converter(model), insertProcessor);
    var socket = connectionProvider.getReadConnection();
    var compressor = socket.getCompressor();
    if (compressor == null && compress != null && compress) {
      throw new IllegalStateException(
              "Compression requested on call but no compressors registered");
    }
    var requestCompression = compressor != null && (compress == null || compress);
    var contextStack = new ContextStack();
    var count = 0;

    while (wrappedItems.hasMore()) {
      var insertRequest =
          new BongoInsertRequest(
              identifier.getCollectionName(), identifier.getDatabaseName(), false);
      var payload =
          BongoPayload.<TModel>builder()
              .identifier("documents")
              .model(model)
              .items(wrappedItems)
              .build();
      wireProtocol.sendCommandMessage(
          socket, codec.converter(BongoInsertRequest.class), insertRequest, requestCompression, false, payload);

      var response = wireProtocol.readServerResponse(socket, codec.converter(BsonDocument.class));

//      System.out.println(response.getPayload());
    }

    socket.release();

//    System.out.println(wrappedItems.getIds());
    return null;
  }
}
