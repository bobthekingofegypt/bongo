package org.bobstuff.bongo.executionstrategy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.converters.BongoWriteRequestConverter;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.messages.BongoBulkWriteResponse;
import org.bobstuff.bongo.messages.BongoWriteRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;

@Slf4j
public class WriteExecutionSerialStrategy<TModel> implements WriteExecutionStrategy<TModel> {
  @Override
  public BongoBulkWriteResult execute(
      BongoCollection.Identifier identifier,
      BongoBulkOperationSplitter<TModel> splitter,
      BongoBulkWriteOptions options,
      BobBsonBufferPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BongoWriteConcern writeConcern) {
    var socket = connectionProvider.getReadConnection();

    var tracker = new BongoBulkWriteTracker(options.isOrdered());
    var allIds = new HashMap<Integer, byte[]>();

    if (options.isOrdered()) {
      writeBulkRequests(
          splitter,
          identifier,
          writeConcern,
          options,
          wireProtocol,
          socket,
          codec,
          tracker,
          allIds);
    } else {
      writeBulkRequestsForType(
          BongoWriteOperationType.values(),
          splitter,
          identifier,
          writeConcern,
          options,
          wireProtocol,
          socket,
          codec,
          tracker,
          allIds);
    }

    socket.release();

    if (tracker.hasErrors()) {
      throw new BongoBulkWriteException(tracker.getWriteErrors());
    }

    if (writeConcern.isAcknowledged()) {
      return new BongoBulkWriteResultAcknowledged(allIds, tracker);
    }
    return new BongoBulkWriteResultUnacknowledged();
  }

  public void writeBulkRequestsForType(
      BongoWriteOperationType[] types,
      BongoBulkOperationSplitter<TModel> splitter,
      BongoCollection.Identifier identifier,
      BongoWriteConcern writeConcern,
      BongoBulkWriteOptions options,
      WireProtocol wireProtocol,
      BongoSocket socket,
      BongoCodec codec,
      BongoBulkWriteTracker tracker,
      Map<Integer, byte[]> allIds) {
    for (var type : types) {
      var queue = new ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>>();
      splitter.drainToQueue(type, queue);
      if (!queue.isEmpty()) {
        final var operations =
            new BongoBulkWriteOperationUnorderedSplitter<>(queue, splitter.getModel(), codec);
        writeBulkRequests(
            operations,
            identifier,
            writeConcern,
            options,
            wireProtocol,
            socket,
            codec,
            tracker,
            allIds);
      }
    }
  }

  public void writeBulkRequests(
      BongoBulkOperationSplitter<TModel> splitter,
      BongoCollection.Identifier identifier,
      BongoWriteConcern writeConcern,
      BongoBulkWriteOptions options,
      WireProtocol wireProtocol,
      BongoSocket socket,
      BongoCodec codec,
      BongoBulkWriteTracker tracker,
      Map<Integer, byte[]> allIds) {
    while (splitter.hasMore()) {
      var operationType = splitter.nextType();
      var request =
          new BongoWriteRequest(operationType, identifier, writeConcern, options.isOrdered(), options.getComment());

      var indexMap = new BongoIndexMap();
      var payload =
          BongoPayload.<TModel>builder()
              .identifier(operationType.getPayload())
              .items(splitter)
              .indexMap(indexMap)
              .build();
      wireProtocol.sendCommandMessage(
          socket,
          // TODO this could be registered on internal bobbson
          new BongoWriteRequestConverter(),
          request,
          options.getCompress(),
          false,
          payload);

      var response =
          wireProtocol.readServerResponse(socket, codec.converter(BongoBulkWriteResponse.class));
      var responsePayload = response.payload();

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
    allIds.putAll(splitter.getIds());
  }

  public void close() {}

  @Override
  public boolean isClosed() {
    return false;
  }
}
