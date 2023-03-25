package org.bobstuff.bongo.executionstrategy;

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.converters.BongoWriteRequestConverter;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.messages.BongoBulkWriteResponse;
import org.bobstuff.bongo.messages.BongoWriteRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;

@Slf4j
public class BulkWriteExecutionSerialStrategy<TModel> {
  public BongoInsertManyResult execute(
      BongoCollection.Identifier identifier,
      BongoBulkOperationSplitter splitter,
      BongoInsertManyOptions options,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BongoWriteConcern writeConcern) {
    var socket = connectionProvider.getReadConnection();
    var requestCompression =
        BongoCompressor.shouldCompress(socket.getCompressor(), options.getCompress());

    var tracker = new BongoBulkWriteTracker(options.isOrdered());

    while (splitter.hasMore()) {
      var operationType = splitter.nextType();
      var request =
          new BongoWriteRequest(operationType, identifier, writeConcern, options.isOrdered());

      var indexMap = new BongoIndexMap();
      var payload =
          BongoPayloadTemp.<TModel>builder()
              .identifier(operationType.getPayload())
              .items(splitter)
                  .indexMap(indexMap)
              .build();
      wireProtocol.sendCommandMessageTemp(
          socket,
          // TODO this could be registered on internal bobbson
          new BongoWriteRequestConverter(),
          request,
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

      tracker.addResponse(operationType, responsePayload, payload.getItems().getIds(), indexMap);

      if (responsePayload.getOk() == 0) {
        System.out.println(responsePayload.getErrmsg());
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

    // TODO DELETE THIS CLASS

    return new BongoInsertManyResultUnacknowledged();
//    if (writeConcern.isAcknowledged()) {
//      return new BongoInsertManyResultAcknowledged(Collections.emptyMap());
//      // TODO figure this back out
//      //      return new BongoInsertManyResultAcknowledged(wrappedItems.getIds());
//    }
//    return new BongoInsertManyResultUnacknowledged();
  }

  public void close() {}

  public boolean isClosed() {
    return false;
  }
}
