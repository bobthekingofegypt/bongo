package org.bobstuff.bongo;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.exception.BongoException;
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
    return this.insertOne(item, BongoInsertOneOptions.builder().build());
  }

  public BongoInsertOneResult insertOne(TModel item, BongoInsertOneOptions options) {
    try (var writeStrategy = new WriteExecutionSerialStrategy<TModel>()) {
      return this.insertOne(item, writeStrategy, options);
    }
  }

  public BongoInsertOneResult insertOne(TModel item, WriteExecutionStrategy<TModel> strategy) {
    return insertOne(item, strategy, BongoInsertOneOptions.builder().build());
  }

  public BongoInsertOneResult insertOne(
      TModel item, WriteExecutionStrategy<TModel> writeStrategy, BongoInsertOneOptions options) {
    var operations = List.of(new BongoInsert<>(item));
    var bulkWriteSplitter = new BongoBulkWriteOperationSplitter<>(operations, model, codec);
    var result =
        writeStrategy.execute(
            identifier,
            bulkWriteSplitter,
            BongoBulkWriteOptions.from(options),
            bufferPool,
            codec,
            connectionProvider,
            wireProtocol,
            writeConcern);

    return configureInsertOneResult(result);
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
    var operations = items.stream().map(BongoInsert::new).toList();
    var bulkWriteSplitter = new BongoBulkWriteOperationSplitter<>(operations, model, codec);
    var result =
        writeStrategy.execute(
            identifier,
            bulkWriteSplitter,
            BongoBulkWriteOptions.from(options),
            bufferPool,
            codec,
            connectionProvider,
            wireProtocol,
            writeConcern);

    return configureInsertManyResult(result);
  }

  private BongoInsertManyResult configureInsertManyResult(BongoBulkWriteResult bulkWriteResult) {
    if (bulkWriteResult.isAcknowledged()) {
      return new BongoInsertManyResultAcknowledged((bulkWriteResult.getInsertedIds()));
    }
    return new BongoInsertManyResultUnacknowledged();
  }

  private BongoInsertOneResult configureInsertOneResult(BongoBulkWriteResult result) {
    if (result.isAcknowledged()) {
      if (result.getInsertedIds().isEmpty()) {
        // TODO add separate exception types for write/concern/command etc
        throw new BongoException("Failed to insert model");
      }
      return new BongoInsertOneResultAcknowledged(result.getInsertedIds().get(0));
    }
    return new BongoInsertOneResultUnacknowledged();
  }

  private BongoUpdateResult configureUpdateResult(BongoBulkWriteResult bulkResult) {
    if (bulkResult.isAcknowledged()) {
      return new BongoUpdateResultAcknowledged(bulkResult);
    }
    return new BongoUpdateResultUnacknowledged();
  }

  private BongoDeleteResult configureDeleteResult(BongoBulkWriteResult bulkResult) {
    if (bulkResult.isAcknowledged()) {
      return new BongoDeleteResultAcknowledged(bulkResult);
    }
    return new BongoDeleteResultUnacknowledged();
  }

  public BongoUpdateResult updateOne(BsonDocument filter, List<BsonDocument> update) {
    return updateOne(filter, update, BongoUpdateOptions.builder().upsert(false).build());
  }

  public BongoUpdateResult updateOne(
      BsonDocument filter, List<BsonDocument> update, BongoUpdateOptions options) {
    try (var strategy = new WriteExecutionSerialStrategy<TModel>()) {
      return updateOne(filter, update, options, strategy);
    }
  }

  public BongoUpdateResult updateOne(
      BsonDocument filter,
      List<BsonDocument> update,
      BongoUpdateOptions options,
      WriteExecutionStrategy<TModel> strategy) {
    var operations = List.of(new BongoUpdate<TModel>(filter, update, false, options));
    var result = bulkWrite(operations, BongoBulkWriteOptions.from(options), strategy);
    return configureUpdateResult(result);
  }

  public BongoUpdateResult updateMany(BsonDocument filter, List<BsonDocument> update) {
    return updateMany(filter, update, BongoUpdateOptions.builder().build());
  }

  public BongoUpdateResult updateMany(
      BsonDocument filter, List<BsonDocument> update, BongoUpdateOptions options) {
    try (var strategy = new WriteExecutionSerialStrategy<TModel>()) {
      return updateMany(filter, update, options, strategy);
    }
  }

  public BongoUpdateResult updateMany(
      BsonDocument filter,
      List<BsonDocument> update,
      BongoUpdateOptions options,
      WriteExecutionStrategy<TModel> strategy) {
    List<BongoWriteOperation<TModel>> operations =
        List.of(new BongoUpdate<>(filter, update, true, options));
    var result = bulkWrite(operations, BongoBulkWriteOptions.from(options), strategy);
    return configureUpdateResult(result);
  }

  public BongoDeleteResult deleteOne(BsonDocument filter) {
    return this.deleteOne(filter, BongoDeleteOptions.builder().build());
  }

  public BongoDeleteResult deleteOne(BsonDocument filter, BongoDeleteOptions options) {
    try (var strategy = new WriteExecutionSerialStrategy<TModel>()) {
      var operations = List.of(new BongoDelete<TModel>(filter, false, options));
      var result = bulkWrite(operations, BongoBulkWriteOptions.from(options), strategy);
      return configureDeleteResult(result);
    }
  }

  public BongoDeleteResult deleteMany(BsonDocument filter) {
    return this.deleteMany(filter, BongoDeleteOptions.builder().build());
  }

  public BongoDeleteResult deleteMany(BsonDocument filter, BongoDeleteOptions options) {
    try (var strategy = new WriteExecutionSerialStrategy<TModel>()) {
      var operations = List.of(new BongoDelete<TModel>(filter, true, options));
      var result = bulkWrite(operations, BongoBulkWriteOptions.from(options), strategy);
      return configureDeleteResult(result);
    }
  }

  public BongoBulkWriteResult bulkWrite(List<? extends BongoWriteOperation<TModel>> operations) {
    return bulkWrite(operations, BongoBulkWriteOptions.builder().build());
  }

  public BongoBulkWriteResult bulkWrite(
      List<? extends BongoWriteOperation<TModel>> operations, BongoBulkWriteOptions options) {
    try (var strategy = new WriteExecutionSerialStrategy<TModel>()) {
      return this.bulkWrite(operations, options, strategy);
    }
  }

  public BongoBulkWriteResult bulkWrite(
      List<? extends BongoWriteOperation<TModel>> operations,
      BongoBulkWriteOptions options,
      WriteExecutionStrategy<TModel> strategy) {
    var bulkWriteSplitter = new BongoBulkWriteOperationSplitter<>(operations, model, codec);
    return strategy.execute(
        identifier,
        bulkWriteSplitter,
        options,
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
