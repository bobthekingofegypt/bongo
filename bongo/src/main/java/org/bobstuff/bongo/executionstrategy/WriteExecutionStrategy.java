package org.bobstuff.bongo.executionstrategy;

import java.util.List;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface WriteExecutionStrategy<TModel> {
  BongoInsertManyResult execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      List<TModel> items,
      BongoInsertManyOptions options,
      @Nullable BongoInsertProcessor<TModel> insertProcessor,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BongoWriteConcern writeConcern);

  void close();
}
