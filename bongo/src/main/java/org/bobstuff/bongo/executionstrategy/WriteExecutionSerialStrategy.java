package org.bobstuff.bongo.executionstrategy;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.messages.BongoBulkWriteResponse;
import org.bobstuff.bongo.messages.BongoInsertRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class WriteExecutionSerialStrategy<TModel> implements WriteExecutionStrategy<TModel> {
  @Override
  public BongoInsertManyResult execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      List<TModel> items,
      BongoInsertManyOptions options,
      @Nullable BongoInsertProcessor<TModel> insertProcessor,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BongoWriteConcern writeConcern) {
    var wrappedItems =
        new BongoWrappedBulkItems<>(items, codec.converter(model), insertProcessor, 0);
    var socket = connectionProvider.getReadConnection();
    var requestCompression =
        BongoCompressor.shouldCompress(socket.getCompressor(), options.getCompress());

    var tracker = new BongoBulkWriteTracker(options.isOrdered());

    while (wrappedItems.hasMore()) {
      var insertRequest =
          new BongoInsertRequest(
              identifier.getCollectionName(),
              identifier.getDatabaseName(),
              writeConcern,
              options.isOrdered());
      var payload =
          BongoPayload.<TModel>builder()
              .identifier("documents")
              .model(model)
              .items(wrappedItems)
              .build();
      wireProtocol.sendCommandMessage(
          socket,
          codec.converter(BongoInsertRequest.class),
          insertRequest,
          requestCompression,
          false,
          payload);

      var response =
          wireProtocol.readServerResponse(socket, codec.converter(BongoBulkWriteResponse.class));
      var responsePayload = response.getPayload();

      if (responsePayload == null) {
        throw new BongoException("Unable to read response to bulk write request");
      }

      if (log.isTraceEnabled()) {
        log.trace(responsePayload.toString());
      }

      tracker.addResponse(responsePayload);

      if (responsePayload.getOk() == 0) {
        // TODO command exception
        throw new BongoException("This should be a command exception");
      } else if (responsePayload.getOk() == 1.0 && tracker.shouldAbort()) {
        throw new BongoBulkWriteException(tracker.getWriteErrors());
      }
    }

    if (tracker.hasErrors()) {
      throw new BongoBulkWriteException(tracker.getWriteErrors());
    }

    socket.release();

    if (writeConcern.isAcknowledged()) {
      return new BongoInsertManyResultAcknowledged(wrappedItems.getIds());
    }
    return new BongoInsertManyResultUnacknowledged();
  }

  public void close() {}
}
