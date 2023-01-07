package org.bobstuff.bongo;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.executionstrategy.ReadExecutionStrategy;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;

public class BongoCollection<TModel> {
  private Identifier identifier;
  private Class<TModel> model;
  private BongoConnectionProvider connectionProvider;
  private BongoCodec codec;

  private WireProtocol wireProtocol;
  private BufferDataPool bufferPool;

  public BongoCollection(
      Identifier identifier,
      Class<TModel> model,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool,
      BongoCodec codec) {
    this.identifier = identifier;
    this.model = model;
    this.connectionProvider = connectionProvider;
    this.wireProtocol = wireProtocol;
    this.codec = codec;
    this.bufferPool = bufferPool;
  }

  public BongoFindIterable<TModel> find(
      BsonDocument filter, BongoFindOptions findOptions, ReadExecutionStrategy readStrategy) {
    return new BongoFindIterable<>(
        identifier, model, connectionProvider, codec, wireProtocol, bufferPool, readStrategy);
  }

  @Data
  @AllArgsConstructor
  public static class Identifier {
    private String databaseName;
    private String collectionName;
  }
}
