package org.bobstuff.bongo.executionstrategy;

import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.converters.BongoFindRequestConverter;
import org.bobstuff.bongo.converters.BongoFindResponseConverter;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.bobstuff.bongo.messages.BongoGetMoreRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class ReadExecutionConcurrentStrategy<TModel> implements ReadExecutionStrategy<TModel> {
  private static final BobBsonBuffer POISON_BUFFER = new BobBufferBobBsonBuffer(new byte[0]);
  private ExecutorService executorService;
  private CompletionService<Void> completionService;
  private BlockingQueue<BobBsonBuffer> decodeQueue;
  private BlockingQueue<BongoFindResponse<TModel>> responses;
  private BobBsonConverter<BongoFindRequest> findRequestConverter;
  private BobBsonConverter<BongoFindResponse<TModel>> findResponseConverter;
  private BobBsonConverter<BongoFindResponse<TModel>> findResponseConverterSkipBody;
  private int parserCount;

  public ReadExecutionConcurrentStrategy(BobBsonConverter<TModel> converter, int parserCount) {
    this.findRequestConverter = new BongoFindRequestConverter();
    this.findResponseConverter = new BongoFindResponseConverter<TModel>(converter, false);
    this.findResponseConverterSkipBody = new BongoFindResponseConverter<TModel>(converter, true);
    this.executorService = Executors.newFixedThreadPool(parserCount + 2);
    this.completionService = new ExecutorCompletionService<>(executorService);
    this.decodeQueue = new ArrayBlockingQueue<>(50);
    this.responses = new ArrayBlockingQueue<>(50);
    this.parserCount = parserCount;
  }

  @Override
  public BongoDbBatchCursor<TModel> execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      @Nullable BongoFindOptions findOptions,
      @Nullable BsonDocument filter,
      @Nullable Boolean compress,
      BongoCursorType cursorType,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BufferDataPool bufferPool,
      BongoConnectionProvider connectionProvider) {
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
            new BongoFindRequest(identifier, findOptions),
            findResponseConverter,
            requestCompression,
            false);

    try {
      responses.put(response.getPayload());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    for (var i = 0; i < parserCount; i += 1) {
      completionService.submit(
          () -> {
            try {
              while (true) {
                BobBsonBuffer buffer = decodeQueue.take();
                if (buffer == POISON_BUFFER) {
                  log.debug("poison decode entry found");
                  break;
                }
                log.debug("decoding a new entry");
                buffer.setHead(5);
                BsonReader reader = new BsonReader(buffer);
                var result = findResponseConverter.read(reader);

                bufferPool.recycle(buffer);
                if (result != null) {
                  responses.put(result);
                }
              }
            } catch (Exception e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
            return null;
          });
    }

    final Future<Void> fetcher =
        executorService.submit(
            () -> {
              BongoFindResponse<TModel> result = null;
              do {
                if (cursorType != BongoCursorType.Exhaustible || result == null) {
                  log.debug("sending fetch more request");
                  var getMoreRequest =
                      new BongoGetMoreRequest(
                          response.getPayload().getId(),
                          identifier.getDatabaseName(),
                          identifier.getCollectionName());

                  log.debug("getmore command fired");

                  wireProtocol.sendCommandMessage(
                      socket,
                      codec.converter(BongoGetMoreRequest.class),
                      getMoreRequest,
                      requestCompression,
                      cursorType == BongoCursorType.Exhaustible);
                }

                WireProtocol.Response<BobBsonBuffer> getMoreResponse;
                log.debug("concurrent strategy reading new batch");
                getMoreResponse = wireProtocol.readRawServerResponse(socket);

                BsonReader reader = new BsonReader(getMoreResponse.getPayload());
                result = findResponseConverterSkipBody.read(reader);
                try {
                  decodeQueue.put(getMoreResponse.getPayload());
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              } while (result != null && result.getOk() == 1.0 && result.getId() != 0);

              return null;
            });

    Future<Void> future =
        executorService.submit(
            () -> {
              fetcher.get();
              log.debug("fetcher is complete adding the end response onto the queue");
              for (var i = 0; i < parserCount; i += 1) {
                decodeQueue.put(POISON_BUFFER);
              }

              int completeCount = 0;
              while (completeCount < parserCount) {
                completionService.take().get();
                completeCount += 1;
              }
              log.debug("parser is complete adding the end response onto the queue");
              responses.put((BongoFindResponse) BongoDbBatchBlockingCursor.END_RESPONSE);

              socket.release();

              return null;
            });

    return new BongoDbBatchBlockingCursor<>(responses);
  }

  public void close() {
    executorService.shutdown();
  }
}
