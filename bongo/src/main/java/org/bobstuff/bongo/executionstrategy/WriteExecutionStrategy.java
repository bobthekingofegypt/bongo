package org.bobstuff.bongo.executionstrategy;

import java.util.List;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.BongoCollection;
import org.bobstuff.bongo.BongoInsertManyResult;
import org.bobstuff.bongo.BongoInsertProcessor;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface WriteExecutionStrategy<TModel> {
  @Nullable BongoInsertManyResult execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      List<TModel> items,
      @Nullable Boolean compress,
      @Nullable BongoInsertProcessor<TModel> insertProcessor,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol);
}
