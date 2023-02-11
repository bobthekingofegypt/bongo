package org.bobstuff.bongo.executionstrategy;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ReadExecutionStrategy<TModel> extends AutoCloseable {
  <RequestModel> BongoDbBatchCursor<TModel> execute(
      BongoCollection.Identifier identifier,
      BobBsonConverter<RequestModel> requestConverter,
      @NonNull RequestModel bongoAggregateRequest,
      Class<TModel> model,
      BongoFindOptions findOptions,
      @Nullable Boolean compress,
      BongoCursorType cursorType,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BufferDataPool bufferPool,
      BongoConnectionProvider connectionProvider);

  void close();

  boolean isClosed();
}
