package org.bobstuff.bongo;

import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.executionstrategy.*;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoCollection<TModel> {
  private Identifier identifier;
  private Class<TModel> model;
  private BongoConnectionProvider connectionProvider;
  private BongoCodec codec;

  private WireProtocol wireProtocol;
  private BufferDataPool bufferPool;

  private BongoWriteConcern writeConcern;

  public BongoCollection(
      Identifier identifier,
      Class<TModel> model,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool,
      BongoCodec codec) {
    this(
        identifier,
        model,
        connectionProvider,
        wireProtocol,
        bufferPool,
        codec,
        new BongoWriteConcern("majority", false));
  }

  public BongoCollection(
      Identifier identifier,
      Class<TModel> model,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool,
      BongoCodec codec,
      BongoWriteConcern writeConcern) {
    this.identifier = identifier;
    this.model = model;
    this.connectionProvider = connectionProvider;
    this.wireProtocol = wireProtocol;
    this.codec = codec;
    this.bufferPool = bufferPool;
    this.writeConcern = writeConcern;
  }

  public BongoCollection<TModel> withWriteConcern(BongoWriteConcern writeConcern) {
    return new BongoCollection<TModel>(
        this.identifier,
        this.model,
        this.connectionProvider,
        this.wireProtocol,
        this.bufferPool,
        this.codec,
        writeConcern);
  }

  public @Nullable TModel findOne() {
    return this.findOne(new BsonDocument());
  }

  public @Nullable TModel findOne(BsonDocument filter) {
    var findIterable = this.find(new ReadExecutionSerialStrategy<>());
    var iterator =
        findIterable.filter(filter).options(BongoFindOptions.builder().limit(1).build()).iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }

  public long count() {
    return this.count(BongoFindOptions.builder().build(), new BsonDocument());
  }

  public long count(BongoFindOptions options, BsonDocument filter) {
    var executor = new BongoCountExecutor();
    return executor.execute(identifier, options, filter, wireProtocol, codec, connectionProvider);
  }

  public BongoAggregateIterable<TModel> aggregate(
      List<BsonDocument> pipeline, ReadExecutionStrategy<TModel> readStrategy) {
    return new BongoAggregateIterable<>(
        identifier,
        pipeline,
        model,
        connectionProvider,
        codec,
        wireProtocol,
        bufferPool,
        readStrategy);
  }

  public <OverrideModel> BongoAggregateIterable<OverrideModel> aggregate(
      List<BsonDocument> pipeline,
      Class<OverrideModel> overrideModel,
      ReadExecutionStrategy<OverrideModel> readStrategy) {
    return new BongoAggregateIterable<>(
        identifier,
        pipeline,
        overrideModel,
        connectionProvider,
        codec,
        wireProtocol,
        bufferPool,
        readStrategy);
  }

  public BongoFindIterable<TModel> find(ReadExecutionStrategy<TModel> readStrategy) {
    return new BongoFindIterable<>(
        identifier, model, connectionProvider, codec, wireProtocol, bufferPool, readStrategy);
  }

  public BongoInsertOneResult insertOne(TModel item) {
    var result = this.insertMany(List.of(item));
    if (writeConcern.isAcknowledged()) {
      return new BongoInsertOneResultAcknowledged(result.getInsertedIds().get(0));
    }
    return new BongoInsertOneResultUnacknowledged();
  }

  public BongoInsertManyResult insertMany(List<TModel> items) {
    return this.insertMany(items, BongoInsertManyOptions.builder().build());
  }

  public BongoInsertManyResult insertMany(List<TModel> items, BongoInsertManyOptions options) {
    try (var writeStrategy = new WriteExecutionSerialStrategy<TModel>()) {
      return this.insertMany(items, writeStrategy, options);
    }
  }

  public BongoInsertManyResult insertMany(
      List<TModel> items, WriteExecutionStrategy<TModel> writeStrategy) {
    return this.insertMany(items, writeStrategy, BongoInsertManyOptions.builder().build());
  }

  public BongoInsertManyResult insertMany(
      List<TModel> items,
      WriteExecutionStrategy<TModel> writeStrategy,
      BongoInsertManyOptions options) {
    return writeStrategy.execute(
        identifier,
        model,
        items,
        options,
        null,
        bufferPool,
        codec,
        connectionProvider,
        wireProtocol,
        writeConcern);
  }

  public void updateOne(BsonDocument filter, List<BsonDocument> update) {
    var strategy = new WriteUpdateExecutionSerialStrategy<BongoUpdate>();
    var bongoUpdate = new BongoUpdate(filter, update, false);
    strategy.execute(
        identifier,
        BongoUpdate.class,
        Collections.singletonList(bongoUpdate),
        BongoInsertManyOptions.builder().build(),
        bufferPool,
        codec,
        connectionProvider,
        wireProtocol,
        writeConcern);
  }

  public void updateMany(BsonDocument filter, List<BsonDocument> update) {
    var strategy = new WriteUpdateExecutionSerialStrategy<BongoUpdate>();
    var bongoUpdate = new BongoUpdate(filter, update, true);
    strategy.execute(
        identifier,
        BongoUpdate.class,
        Collections.singletonList(bongoUpdate),
        BongoInsertManyOptions.builder().build(),
        bufferPool,
        codec,
        connectionProvider,
        wireProtocol,
        writeConcern);
  }

  public void bulkWrite(List<? extends BongoWriteOperation<TModel>> operations) {
    var strategy = new BulkWriteExecutionSerialStrategy<BongoUpdate>();
    //    var bongoUpdate = new BongoUpdate(filter, update, true);
    var bulkWriteSplitter = new BongoBulkWriteOperationSplitter<TModel>(operations, model, codec);
    strategy.execute(
        identifier,
        bulkWriteSplitter,
        BongoInsertManyOptions.builder().compress(false).build(),
        bufferPool,
        codec,
        connectionProvider,
        wireProtocol,
        writeConcern);
  }

  @Data
  @AllArgsConstructor
  public static class Identifier {
    private String databaseName;
    private String collectionName;
  }
}
