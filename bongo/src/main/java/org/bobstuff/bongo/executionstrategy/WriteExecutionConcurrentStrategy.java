package org.bobstuff.bongo.executionstrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.DynamicBobBsonBuffer;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.messages.BongoInsertRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WriteExecutionConcurrentStrategy<TModel> implements WriteExecutionStrategy<TModel> {
  @SuppressWarnings("argument")
  private static final DynamicBobBsonBuffer POISON_BUFFER =
      new DynamicBobBsonBuffer(Arrays.asList(new BobBufferBobBsonBuffer(new byte[0])), null);

  private BlockingQueue<DynamicBobBsonBuffer> messageQueue;
  private ExecutorService executorService;
  private CompletionService<Void> writersCompletionService;
  private CompletionService<Void> sendersCompletionService;
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
    var socket = connectionProvider.getReadConnection();
    var compressor = socket.getCompressor();
    if (compressor == null && compress != null && compress) {
      throw new IllegalStateException(
          "Compression requested on call but no compressors registered");
    }
    var requestCompression = compressor != null && (compress == null || compress);

    var subListSize = (int) Math.ceil(items.size() / (float) writers);
    for (var i = 0; i < writers; i += 1) {
      final List<TModel> subList =
          items.subList(i * subListSize, Math.min(((i + 1) * subListSize), items.size()));
      writersCompletionService.submit(
          () -> {
            var wrappedItems =
                new BongoWrappedBulkItems<>(subList, codec.converter(model), insertProcessor);
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

            return null;
          });
    }

    for (var i = 0; i < senders; i += 1) {
      sendersCompletionService.submit(
          () -> {
            var localSocket = connectionProvider.getReadConnection();
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
                  wireProtocol.readServerResponse(localSocket, codec.converter(BsonDocument.class));
            }

            localSocket.release();

            return null;
          });
    }

    var done = 0;
    while (done < writers) {
      try {
        writersCompletionService.take().get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
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

    done = 0;
    while (done < senders) {
      try {
        sendersCompletionService.take().get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }

      done += 1;
    }

    socket.release();
    return null;
  }

  public void close() {
    executorService.shutdown();
  }
}
