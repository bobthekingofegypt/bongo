package org.bobstuff.bongo.executionstrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

// TODO Implement error handling
// TODO implement integrated count query for find all style queries
public class ReadExecutionConcurrentFetchers<TModel> implements ReadExecutionStrategy<TModel> {
  private int fetchers;
  private ExecutorService fetchersExecutorService;
  private CompletionService<List<TModel>> fetchersCompletionService;

  private FetcherStrategyProvider<TModel> strategyProvider;

  public ReadExecutionConcurrentFetchers(int fetchers) {
    this(fetchers, ReadExecutionSerialStrategy::new);
  }

  public ReadExecutionConcurrentFetchers(
      int fetchers, FetcherStrategyProvider<TModel> strategyProvider) {
    this.fetchers = fetchers;
    this.fetchersExecutorService = Executors.newFixedThreadPool(fetchers);
    this.fetchersCompletionService = new ExecutorCompletionService<>(fetchersExecutorService);
    this.strategyProvider = strategyProvider;
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
    var skip = 0;
    var limit = 0;
    if (findOptions != null) {
      limit = findOptions.getLimit();
      skip = findOptions.getSkip();
    } else {
      throw new BongoException("find options cannot be null");
    }

    if (limit == 0) {
      // we need to do a count query to get the number of results
      // TODO we need access to collection instance here
      throw new UnsupportedOperationException("Only supporting calls with a limit clause for now");
    }

    var batchSize = limit / (fetchers);

    for (var i = 0; i < fetchers; i += 1) {
      final var offset = skip + (i * batchSize);
      final var limitedBatchSize = i == fetchers - 1 ? limit - offset : batchSize;
      fetchersCompletionService.submit(
          () -> {
            var results = new ArrayList<TModel>();
            try (var strategy = strategyProvider.provide()) {
              var cursor =
                  strategy.execute(
                      identifier,
                      model,
                      findOptions.withLimit(limitedBatchSize).withSkip(offset),
                      filter,
                      compress,
                      cursorType,
                      wireProtocol,
                      codec,
                      bufferPool,
                      connectionProvider);
              while (cursor.hasNext()) {
                var next = cursor.next();
                results.addAll(next);
              }
              cursor.close();
            }
            return results;
          });
    }

    var totalResults = new ArrayList<TModel>();
    var complete = 0;
    while (complete != fetchers) {
      try {
        var future = fetchersCompletionService.take();
        totalResults.addAll(future.get());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
      complete += 1;
    }

    return new BongoDbBatchCursorList<>(totalResults);
  }

  @Override
  public void close() {
    fetchersExecutorService.shutdownNow();
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @FunctionalInterface
  public interface FetcherStrategyProvider<TModel> {
    ReadExecutionStrategy<TModel> provide();
  }
}
