package org.bobstuff.bongo.executionstrategy;

import org.bobstuff.bongo.BongoCollection;
import org.bobstuff.bongo.BongoDbBatchCursor;
import org.bobstuff.bongo.BongoFindOptions;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ReadExecutionStrategy<TModel> {
  public BongoDbBatchCursor<TModel> execute(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      @Nullable BongoFindOptions findOptions,
      @Nullable BsonDocument filter,
      WireProtocol wireProtocol,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider);
}
