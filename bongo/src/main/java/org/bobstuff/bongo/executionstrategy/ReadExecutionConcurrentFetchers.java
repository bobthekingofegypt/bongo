package org.bobstuff.bongo.executionstrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.NonNull;
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
  public <RequestModel> BongoDbBatchCursor<TModel> execute(
      BongoCollection.Identifier identifier,
      BobBsonConverter<RequestModel> requestConverter,
      @NonNull RequestModel bongoRequest,
      Class<TModel> model,
      BongoFindOptions findOptions,
      @Nullable Boolean compress,
      BongoCursorType cursorType,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BufferDataPool bufferPool,
      BongoConnectionProvider connectionProvider) {
    if (!(bongoRequest instanceof BongoFindRequest)) {
      throw new UnsupportedOperationException(
          "Mutliple fetchers strategy is currently only compatible with find requests");
    }

    var request = (BongoFindRequest) bongoRequest;
    var skip = findOptions.getLimit();
    var limit = findOptions.getSkip();

    if (limit == 0) {
      var filter = request.getFilter();
      if (filter == null) {
        filter = new BsonDocument();
      }
      var countExecutor = new BongoCountExecutor();
      limit =
          (int)
              countExecutor.execute(
                  identifier, findOptions, filter, wireProtocol, codec, connectionProvider);
    }

    var batchSize = limit / (fetchers);

    for (var i = 0; i < fetchers; i += 1) {
      final var offset = skip + (i * batchSize);
      final var limitedBatchSize = i == fetchers - 1 ? limit - offset : batchSize;
      fetchersCompletionService.submit(
          () -> {
            var results = new ArrayList<TModel>();
            var fo = findOptions.withLimit(limitedBatchSize).withSkip(offset);
            @SuppressWarnings("unchecked")
            RequestModel filteredRequest = (RequestModel) request.withFindOptions(fo);
            if (filteredRequest == null) {
              throw new IllegalStateException(
                  "Checker insists that request be non null, I don't see how it could be null");
            }
            try (var strategy = strategyProvider.provide()) {
              var cursor =
                  strategy.execute(
                      identifier,
                      requestConverter,
                      filteredRequest,
                      model,
                      fo,
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
