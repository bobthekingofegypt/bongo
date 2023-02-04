package org.bobstuff.bongo.executionstrategy;

import static org.bobstuff.bongo.BongoDbBatchBlockingCursor.*;

import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.*;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.converters.BongoFindRequestConverter;
import org.bobstuff.bongo.converters.BongoFindResponseConverter;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.exception.BongoReadException;
import org.bobstuff.bongo.executionstrategy.read.concurrent.DecodingCallable;
import org.bobstuff.bongo.executionstrategy.read.concurrent.FetcherCallable;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.bobstuff.bongo.messages.BongoGetMoreRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class ReadExecutionConcurrentStrategy<TModel> implements ReadExecutionStrategy<TModel> {
  private final ExecutorService processingExecutorService;
  private final ExecutorService monitorExecutionService;
  private final CompletionService<Void> processingCompletionService;
  private final BlockingQueue<BobBsonBuffer> decodeQueue;
  private final BlockingQueue<BongoFindResponse<TModel>> responses;
  private final int parserCount;

  private @Nullable BongoDbBatchBlockingCursor<TModel> cursor;
  private @Nullable BongoSocket socket;
  private boolean exhaustible;
  private boolean closed;
  private boolean aborted;
  private boolean started;

  public ReadExecutionConcurrentStrategy(int parserCount) {
    this(parserCount, 50);
  }

  public ReadExecutionConcurrentStrategy(int parserCount, int queueCapacity) {
    this.processingExecutorService = Executors.newFixedThreadPool(parserCount + 4);
    this.monitorExecutionService = Executors.newSingleThreadExecutor();
    this.processingCompletionService = new ExecutorCompletionService<>(processingExecutorService);
    this.decodeQueue = new ArrayBlockingQueue<>(queueCapacity);
    this.responses = new ArrayBlockingQueue<>(queueCapacity);
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
    if (closed || aborted || started) {
      throw new BongoException("Attempt to reuse ReadExecutionStrategy");
    }
    this.started = true;

    var modelConverter = codec.converter(model);
    var findRequestConverter = new BongoFindRequestConverter(codec.converter(BsonDocument.class));
    var findResponseConverter = new BongoFindResponseConverter<>(modelConverter, false);
    var findResponseConverterSkipBody = new BongoFindResponseConverter<>(modelConverter, true);
    var socket = connectionProvider.getReadConnection();
    var requestCompression = BongoCompressor.shouldCompress(socket.getCompressor(), compress);

    // send the first request with no concurrency
    var response =
        wireProtocol.sendReceiveCommandMessage(
            socket,
            findRequestConverter,
            new BongoFindRequest(identifier, findOptions, filter),
            findResponseConverter,
            requestCompression,
            false);

    var payload = response.getPayload();
    if (!payload.isOk()) {
      log.trace("Response from initial find contains non ok code {}", payload.getOk());
      socket.release();
      throw new BongoReadException(payload.getCode(), payload.getErrmsg(), payload.getCodeName());
    }

    try {
      responses.put(response.getPayload());
    } catch (InterruptedException e) {
      throw new BongoException(
          "Failed to put initial find response onto response queue due to interrupt", e);
    }

    if (!payload.hasMore()) {
      log.trace("No more data after first response, avoiding starting threads");
      try {
        return new BongoDbBatchCursorSerial<>(payload, model, new NoOpGetMore());
      } finally {
        socket.release();
      }
    }

    // initial request has worked, more responses are expected so start up the threads

    for (var i = 0; i < parserCount; i += 1) {
      processingCompletionService.submit(
          new DecodingCallable<>(decodeQueue, responses, findResponseConverter, bufferPool));
    }

    this.exhaustible = cursorType == BongoCursorType.Exhaustible;
    var getMoreRequest =
        new BongoGetMoreRequest(
            response.getPayload().getId(),
            identifier.getDatabaseName(),
            identifier.getCollectionName(),
            findOptions != null ? findOptions.getBatchSize() : null);

    final Future<Void> fetcher =
        processingExecutorService.submit(
            new FetcherCallable<>(
                this.exhaustible,
                getMoreRequest,
                socket,
                wireProtocol,
                requestCompression,
                findResponseConverterSkipBody,
                codec.converter(BongoGetMoreRequest.class),
                decodeQueue));

    Future<Void> monitor =
        monitorExecutionService.submit(
            () -> {
              while (true) {
                try {
                  fetcher.get(100, TimeUnit.MILLISECONDS);
                  break;
                } catch (TimeoutException e) {
                  log.trace(
                      "Timeout waiting for fetcher to complete proceeding to check parser threads");
                } catch (InterruptedException e) {
                  return null;
                } catch (Throwable e) {
                  abort(e);
                  throw e;
                }

                var result = processingCompletionService.poll();
                if (result != null) {
                  // any complete processor is a problem at this point so we will abort
                  try {
                    result.get();
                  } catch (Exception e) {
                    abort(e);
                    throw e;
                  }
                }
              }

              if (Thread.interrupted()) {
                return null;
              }

              log.trace(
                  "fetching is complete and no current errors in processing so adding the end"
                      + " response onto the queue");
              for (var i = 0; i < parserCount; i += 1) {
                decodeQueue.put(DecodingCallable.POISON_BUFFER);
              }

              int completeCount = 0;
              while (completeCount < parserCount) {
                try {
                  processingCompletionService.take().get();
                } catch (InterruptedException e) {
                  log.trace(
                      "interrupted waiting for processing completion, probably due to failed"
                          + " shutdown process");
                  throw e;
                } catch (ExecutionException e) {
                  abort(e.getCause());
                  throw e;
                }
                completeCount += 1;
              }
              log.debug(
                  "parser is complete adding end response onto the queue for batch cursor to"
                      + " pickup");
              submitResponseQueuePoisonPill();

              return null;
            });

    this.socket = socket;
    this.cursor = new BongoDbBatchBlockingCursor<>(responses, this);
    return this.cursor;
  }

  public void close() {
    if (!closed) {
      closed = true;
      this.cursor = null;
      if (!processingExecutorService.isTerminated()) {
        if (!processingExecutorService.isShutdown()) {
          processingExecutorService.shutdownNow();
        }
        try {
          var success = processingExecutorService.awaitTermination(600, TimeUnit.MILLISECONDS);
          if (success) {
            log.trace("successfully shutdown processing executor service");
          } else {
            log.trace("failed to successfully shutdown processing executor service");
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      monitorExecutionService.shutdownNow();
      try {
        var success = monitorExecutionService.awaitTermination(6000, TimeUnit.SECONDS);
        if (success) {
          log.trace("successfully shutdown monitor service");
        } else {
          log.trace("failed to successfully shutdown monitor service");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        var localSocket = this.socket;
        if (localSocket != null) {
          if (exhaustible && aborted) {
            localSocket.close();
          } else {
            localSocket.release();
          }
          this.socket = null;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void abort(@Nullable Throwable triggerException) {
    log.trace("Aborting concurrent strategy");
    if (!aborted) {
      if (triggerException != null) {
        var cursor = this.cursor;
        if (cursor != null) {
          cursor.setTriggeringException(triggerException);
        }
      }
      processingExecutorService.shutdownNow();
      try {
        var success = processingExecutorService.awaitTermination(6000, TimeUnit.SECONDS);
        if (success) {
          log.trace("successfully shutdown processing queues");
        } else {
          log.trace("failed to successfully shutdown processing queues");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          responses.clear();
          responses.put((BongoFindResponse<TModel>) ABORT_RESPONSE);
        } catch (InterruptedException e) {
          log.warn("interrupted when trying to issue abort response onto responses queue");
        }
      }
      aborted = true;
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @SuppressWarnings("unchecked")
  private void submitResponseQueuePoisonPill() {
    try {
      responses.put((BongoFindResponse<TModel>) END_RESPONSE);
    } catch (InterruptedException e) {
      throw new BongoException(
          "Failed to put poison pill onto responses queue due to interrupt", e);
    }
  }

  private class NoOpGetMore implements BongoDbBatchCursorGetMore<TModel> {
    @Override
    public @Nullable BongoFindResponse<TModel> getMore() {
      return null;
    }

    @Override
    public void abort() {}
  }
}
