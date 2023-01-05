package org.bobstuff.bongo;

import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.executionstrategy.ReadExecutionStrategy;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoFindIterable<TModel> {
  private BongoCollection.Identifier identifier;
  private Class<TModel> model;
  private @Nullable BongoFindOptions findOptions;
  private @Nullable BsonDocument filter;

  private BongoConnectionProvider connectionProvider;
  private BongoCodec codec;

  private ReadExecutionStrategy readExecutionStrategy;

  private WireProtocol wireProtocol;

  public BongoFindIterable(
      BongoCollection.Identifier identifier,
      Class<TModel> model,
      BongoConnectionProvider connectionProvider,
      BongoCodec codec,
      WireProtocol wireProtocol,
      ReadExecutionStrategy readExecutionStrategy) {
    this.identifier = identifier;
    this.model = model;
    this.codec = codec;
    this.connectionProvider = connectionProvider;
    this.wireProtocol = wireProtocol;
    this.readExecutionStrategy = readExecutionStrategy;
  }

  public BongoFindIterable<TModel> options(BongoFindOptions options) {
    this.findOptions = options;
    return this;
  }

  public BongoFindIterable<TModel> filter(BsonDocument filter) {
    this.filter = filter;
    return this;
  }

  public BongoCursor<TModel> cursor() {
    // execute first call, pass results to cursor so it can issue subsequent requests
    return new BongoCursor<TModel>(
        readExecutionStrategy.execute(
            identifier, model, findOptions, filter, wireProtocol, codec, connectionProvider));
  }
}
