package org.bobstuff.bongo.executionstrategy;

import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.converters.BongoFindRequestConverter;
import org.bobstuff.bongo.converters.BongoFindResponseConverter;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.bobstuff.bongo.messages.BongoGetMoreRequest;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class ReadExecutionConcurrentCompStrategy<TModel> implements ReadExecutionStrategy<TModel> {
  private static final WireProtocol.Response<BobBsonBuffer> POISON_BUFFER =
      new WireProtocol.Response<>(
          new BongoResponseHeader(), 0, new BobBufferBobBsonBuffer(new byte[0]));
  private ExecutorService executorService;
  private CompletionService<Void> completionService;
  private BlockingQueue<WireProtocol.Response<BobBsonBuffer>> decodeQueue;
  private BlockingQueue<BongoFindResponse<TModel>> responses;
  private int parserCount;

  private boolean closed;

  public ReadExecutionConcurrentCompStrategy(int parserCount) {
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
    if (closed) {
      throw new BongoException("Attempt to use closed ReadExecutionStrategy");
    }
    var findRequestConverter = new BongoFindRequestConverter(codec.converter(BsonDocument.class));
    var findResponseConverter =
        new BongoFindResponseConverter<TModel>(codec.converter(model), false);
    var findResponseConverterSkipBody =
        new BongoFindResponseConverter<TModel>(codec.converter(model), true);
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
            false,
            false);

    if (response.getPayload().getOk() == 0.0) {
      throw new BongoException(response.getPayload().getErrmsg());
    }
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
                var re = decodeQueue.take();
                if (re == POISON_BUFFER) {
                  log.debug("poison decode entry found");
                  break;
                }
                log.debug("decoding a new entry");
                if (re.getHeader().getOpCode() == 2013) {
                  var buffer = re.getPayload();
                  buffer.setHead(5);
                  BsonReader reader = new BsonReader(buffer);
                  var result = findResponseConverter.read(reader);

                  bufferPool.recycle(buffer);
                  if (result != null) {
                    responses.put(result);
                  }
                } else {
                  var decompressedMessageBuffer =
                      bufferPool.allocate(re.getHeader().getMessageLength());
                  var decompressedMessageBufferArray = decompressedMessageBuffer.getArray();
                  if (decompressedMessageBufferArray == null) {
                    throw new BongoException("Buffers internal array is not accessible");
                  }
                  var messageBufferArray = re.getPayload().getArray();
                  if (messageBufferArray == null) {
                    throw new BongoException("Buffers internal array is not accessible");
                  }
                  if (compressor == null) {
                    throw new RuntimeException();
                  }
                  compressor.decompress(
                      messageBufferArray,
                      re.getPayload().getHead(),
                      re.getPayload().getReadRemaining(),
                      decompressedMessageBufferArray,
                      decompressedMessageBuffer.getHead(),
                      re.getHeader().getMessageLength());
                  bufferPool.recycle(re.getPayload());
                  decompressedMessageBuffer.setTail(re.getHeader().getMessageLength());
                  decompressedMessageBuffer.setHead(5);
                  BsonReader reader = new BsonReader(decompressedMessageBuffer);
                  var result = findResponseConverter.read(reader);

                  bufferPool.recycle(decompressedMessageBuffer);
                  if (result != null) {
                    responses.put(result);
                  }
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
              WireProtocol.Response<BobBsonBuffer> getMoreResponse = null;
              do {
                //                  System.out.println("TESTING");
                if (cursorType != BongoCursorType.Exhaustible || getMoreResponse == null) {
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

                log.debug("concurrent strategy reading new batch");
                getMoreResponse = wireProtocol.readRawServerResponse(socket, false);

                //                BsonReader reader = new BsonReader(getMoreResponse.getPayload());
                //                result = findResponseConverterSkipBody.read(reader);
                try {
                  decodeQueue.put(getMoreResponse);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              } while (getMoreResponse.getFlagBits() == 2);

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
    if (!closed) {
      executorService.shutdown();
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(10, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
