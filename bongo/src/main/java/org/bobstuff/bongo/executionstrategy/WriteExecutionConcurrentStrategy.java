package org.bobstuff.bongo.executionstrategy;

import java.util.*;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.DynamicBobBsonBuffer;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
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
public class WriteExecutionConcurrentStrategy<TModel> implements WriteExecutionStrategy<TModel> {
  @SuppressWarnings("argument")
  private static final DynamicBobBsonBuffer POISON_BUFFER =
      new DynamicBobBsonBuffer(List.of(new BobBufferBobBsonBuffer(new byte[0])), null);

  private BlockingQueue<DynamicBobBsonBuffer> messageQueue;
  private ExecutorService executorService;
  private CompletionService<Map<Integer, byte[]>> writersCompletionService;
  private CompletionService<BongoBulkWriteTracker> sendersCompletionService;
  private int writers;
  private int senders;

  public WriteExecutionConcurrentStrategy(int writers, int senders) {
    executorService = Executors.newFixedThreadPool(writers + senders);
    writersCompletionService = new ExecutorCompletionService<>(executorService);
    sendersCompletionService = new ExecutorCompletionService<>(executorService);
    messageQueue = new ArrayBlockingQueue<>(40);
    this.writers = writers;
    this.senders = senders;
  }

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
    // TODO only allow this strategy is ordered is false when number of senders > 1
    var socket = connectionProvider.getReadConnection();
    var requestCompression =
        BongoCompressor.shouldCompress(socket.getCompressor(), options.getCompress());

    var subListSize = (int) Math.ceil(items.size() / (float) writers);
    for (var i = 0; i < writers; i += 1) {
      final int indexOffset = i * subListSize;
      final List<TModel> subList =
          items.subList(indexOffset, Math.min(((i + 1) * subListSize), items.size()));
      writersCompletionService.submit(
          () -> {
            var wrappedItems =
                new BongoWrappedBulkItems<>(
                    subList, codec.converter(model), insertProcessor, indexOffset);
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

              var requestId = socket.getNextRequestId();
              var buffer =
                  wireProtocol.prepareCommandMessage(
                      socket,
                      codec.converter(BongoInsertRequest.class),
                      insertRequest,
                      requestCompression,
                      false,
                      payload,
                      requestId);

              messageQueue.put(buffer);
            }

            return wrappedItems.getIds();
          });
    }

    for (var i = 0; i < senders; i += 1) {
      sendersCompletionService.submit(
          () -> {
            var localSocket = connectionProvider.getReadConnection();
            var tracker = new BongoBulkWriteTracker(options.isOrdered());
            while (true) {
              DynamicBobBsonBuffer buffer = null;
              try {
                buffer = messageQueue.take();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }

              if (buffer == POISON_BUFFER) {
                break;
              }

              for (BobBsonBuffer b : buffer.getBuffers()) {
                localSocket.write(b);
              }

              buffer.release();

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

              tracker.addResponse(responsePayload);
            }

            localSocket.release();

            return tracker;
          });
    }

    var allIds = new HashMap<Integer, byte[]>();

    var done = 0;
    while (done < writers) {
      try {
        var ids = writersCompletionService.take().get();
        allIds.putAll(ids);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      done += 1;
    }

    try {
      for (var i = 0; i < senders; i += 1) {
        messageQueue.put(POISON_BUFFER);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    var combinedTracker = new BongoBulkWriteTracker(options.isOrdered());
    done = 0;
    while (done < senders) {
      try {
        var tracker = sendersCompletionService.take().get();
        combinedTracker.mergeTracker(tracker);
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      done += 1;
    }

    if (combinedTracker.hasErrors()) {
      throw new BongoBulkWriteException(combinedTracker.getWriteErrors());
    }

    socket.release();
    if (writeConcern.isAcknowledged()) {
      return new BongoInsertManyResultAcknowledged(allIds);
    }
    return new BongoInsertManyResultUnacknowledged();
  }

  public void close() {
    executorService.shutdown();
  }
}
