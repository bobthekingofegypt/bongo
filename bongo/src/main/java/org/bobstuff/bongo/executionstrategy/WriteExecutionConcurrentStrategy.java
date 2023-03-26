package org.bobstuff.bongo.executionstrategy;

import java.util.*;
import java.util.concurrent.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.DynamicBobBsonBuffer;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.converters.BongoWriteRequestConverter;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.messages.BongoBulkWriteResponse;
import org.bobstuff.bongo.messages.BongoWriteRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;

@Slf4j
public class WriteExecutionConcurrentStrategy<TModel> implements WriteExecutionStrategy<TModel> {
  @SuppressWarnings("argument")
  private static final BufferWithType POISON_BUFFER =
      new BufferWithType(
          BongoWriteOperationType.Insert,
          new DynamicBobBsonBuffer(List.of(new BobBufferBobBsonBuffer(new byte[0])), null),
          new BongoIndexMap());

  private BlockingQueue<BufferWithType> messageQueue;
  private ExecutorService executorService;
  private CompletionService<Map<Integer, byte[]>> writersCompletionService;
  private CompletionService<BongoBulkWriteTracker> sendersCompletionService;
  private int writers;
  private int senders;

  private boolean closed;

  public WriteExecutionConcurrentStrategy(int writers, int senders) {
    executorService = Executors.newFixedThreadPool(writers + senders);
    writersCompletionService = new ExecutorCompletionService<>(executorService);
    sendersCompletionService = new ExecutorCompletionService<>(executorService);
    messageQueue = new ArrayBlockingQueue<>(40);
    this.writers = writers;
    this.senders = senders;
  }

  @Override
  public BongoBulkWriteResult execute(
      BongoCollection.Identifier identifier,
      BongoBulkOperationSplitter<TModel> splitter,
      BongoInsertManyOptions options,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BongoWriteConcern writeConcern) {
    if (closed) {
      throw new BongoException("Attempt to use closed WriteExecutionStrategy");
    }

    if (options.isOrdered() && (writers > 1 || senders > 1)) {
      throw new BongoException(
          "Cannot run concurrent strategy on an ordered write operation with more than 1 writer or"
              + " sender.  You can use concurrent strategy to run request serialisation and sending"
              + " on independent threads but writers and senders must be 1 to maintain order");
    }

    var socket = connectionProvider.getReadConnection();
    var requestCompression =
        BongoCompressor.shouldCompress(socket.getCompressor(), options.getCompress());

    for (var i = 0; i < senders; i += 1) {
      sendersCompletionService.submit(
          () -> {
            var localSocket = connectionProvider.getReadConnection();
            var tracker = new BongoBulkWriteTracker(options.isOrdered());
            while (true) {
              BufferWithType bufferWithType;
              try {
                bufferWithType = messageQueue.take();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }

              if (bufferWithType == POISON_BUFFER) {
                break;
              }

              for (BobBsonBuffer b : bufferWithType.getBuffer().getBuffers()) {
                localSocket.write(b);
              }

              bufferWithType.getBuffer().release();

              var response =
                  wireProtocol.readServerResponse(
                      localSocket, codec.converter(BongoBulkWriteResponse.class));
              var responsePayload = response.getPayload();

              if (responsePayload == null) {
                throw new BongoException("Unable to read response to bulk write request");
              }

              if (log.isTraceEnabled()) {
                log.trace(responsePayload.toString());
              }

              // TODO handle failure here.  If there is a command failure or a writeconcern failure
              // should we abort all?

              tracker.addResponse(
                  bufferWithType.getType(),
                  responsePayload,
                  Collections.emptyMap(),
                  bufferWithType.getIndexMap());
            }

            localSocket.release();

            return tracker;
          });
    }

    // TODO fork logic for 1-1 ordered concurrent

    //    System.out.println("+++++++++++++");
    var allIds = new HashMap<Integer, byte[]>();

    if (writers == 1 && senders == 1 && options.isOrdered()) {
      // If we are one to one and ordered we need to write the future requests in
      // order breaking on each change of request type, so use an ordered splitter
      // as it was sent to the strategy
      writeBulkRequests(
          splitter,
          identifier,
          writeConcern,
          requestCompression,
          options,
          wireProtocol,
          socket,
          allIds);
    } else {
      var insertsQueue = new ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>>();
      splitter.drainToQueue(BongoWriteOperationType.Insert, insertsQueue);
      //      System.out.println("+++++++++++++ inserts " + insertsQueue.size());
      if (!insertsQueue.isEmpty()) {
        final var inserts =
            new BongoBulkWriteOperationUnorderedSplitter<>(
                insertsQueue, splitter.getModel(), codec);

        writeBulkRequests(
            inserts,
            identifier,
            writeConcern,
            requestCompression,
            options,
            wireProtocol,
            socket,
            allIds);
      }

      //      System.out.println("+++++++++++++ 3");
      var updatesQueue = new ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>>();
      splitter.drainToQueue(BongoWriteOperationType.Update, updatesQueue);
      //      System.out.println("+++++++++++++ updates " + updatesQueue.size());
      if (!updatesQueue.isEmpty()) {
        final var updates =
            new BongoBulkWriteOperationUnorderedSplitter<>(
                updatesQueue, splitter.getModel(), codec);
        writeBulkRequests(
            updates,
            identifier,
            writeConcern,
            requestCompression,
            options,
            wireProtocol,
            socket,
            allIds);
      }

      var deletesQueue = new ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>>();
      splitter.drainToQueue(BongoWriteOperationType.Delete, deletesQueue);
      //      System.out.println("+++++++++++++ deletes " + deletesQueue.size());
      if (!deletesQueue.isEmpty()) {
        final var deletes =
            new BongoBulkWriteOperationUnorderedSplitter<>(
                deletesQueue, splitter.getModel(), codec);
        writeBulkRequests(
            deletes,
            identifier,
            writeConcern,
            requestCompression,
            options,
            wireProtocol,
            socket,
            allIds);
      }
    }
    //    var tracker = new BongoBulkWriteTracker(options.isOrdered());

    //    var subListSize = (int) Math.ceil(items.size() / (float) writers);

    try {
      for (var i = 0; i < senders; i += 1) {
        messageQueue.put(POISON_BUFFER);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    //    System.out.println("$$$%%%%%%%%%%%%%%%%%%");
    allIds.putAll(splitter.getIds());
    var combinedTracker = new BongoBulkWriteTracker(options.isOrdered(), allIds);
    var done = 0;
    while (done < senders) {
      try {
        var tracker = sendersCompletionService.take().get();
        combinedTracker.mergeTracker(tracker);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      done += 1;
    }

    socket.release();

    if (combinedTracker.hasErrors()) {
      throw new BongoBulkWriteException(combinedTracker.getWriteErrors());
    }

    if (writeConcern.isAcknowledged()) {
      return new BongoBulkWriteResultAcknowledged(allIds, combinedTracker);
    }
    return new BongoBulkWriteResultUnacknowledged();
  }

  private void writeBulkRequests(
      BongoBulkOperationSplitter<TModel> splitter,
      BongoCollection.Identifier identifier,
      BongoWriteConcern writeConcern,
      boolean requestCompression,
      BongoInsertManyOptions options,
      WireProtocol wireProtocol,
      BongoSocket socket,
      Map<Integer, byte[]> allIds) {
    for (var i = 0; i < writers; i += 1) {
      writersCompletionService.submit(
          () -> {
            while (splitter.hasMore()) {
              var type = splitter.nextType();
              var request =
                  new BongoWriteRequest(type, identifier, writeConcern, options.isOrdered());

              var indexMap = new BongoIndexMap();

              var payload =
                  BongoPayloadTemp.<TModel>builder()
                      .identifier(type.getPayload())
                      .items(splitter)
                      .indexMap(indexMap)
                      .build();

              var requestId = socket.getNextRequestId();
              var buffer =
                  wireProtocol.prepareCommandMessageTemp(
                      socket,
                      new BongoWriteRequestConverter(),
                      request,
                      requestCompression,
                      false,
                      payload,
                      requestId);

              messageQueue.put(new BufferWithType(type, buffer, indexMap));
            }

            return splitter.getIds();
          });
    }

    var done = 0;
    while (done < writers) {
      try {
        var ids = writersCompletionService.take().get();
        //        allIds.putAll(ids);
      } catch (InterruptedException | ExecutionException e) {
        //        System.out.println("WTF");
        throw new RuntimeException(e);
      }

      done += 1;
    }

    allIds.putAll(splitter.getIds());
  }

  public void close() {
    if (!closed) {
      executorService.shutdown();
    }
    closed = true;
  }

  //  @Override
  public boolean isClosed() {
    return closed;
  }

  public int getWriters() {
    return writers;
  }

  public int getSenders() {
    return senders;
  }

  @Data
  @AllArgsConstructor
  public static class BufferWithType {
    private BongoWriteOperationType type;
    private DynamicBobBsonBuffer buffer;
    private BongoIndexMap indexMap;
  }
}
