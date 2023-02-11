package org.bobstuff.bongo;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.BongoCollection.Identifier;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.converters.BongoAggregateRequestConverter;
import org.bobstuff.bongo.converters.BongoFindRequestConverter;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.executionstrategy.ReadExecutionStrategy;
import org.bobstuff.bongo.messages.BongoAggregateRequest;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoAggregateIterable<TModel> {
  private final Identifier identifier;
  private final Class<TModel> model;
  private @Nullable Integer batchSize;

  private long maxTimeMS;
  private @Nullable Boolean compress;
  private BongoCursorType cursorType = BongoCursorType.Default;
  private final BongoConnectionProvider connectionProvider;
  private final BongoCodec codec;

  private final ReadExecutionStrategy<TModel> readExecutionStrategy;
  private final BufferDataPool bufferPool;
  private final WireProtocol wireProtocol;
  private final List<BsonDocument> pipeline;

  public BongoAggregateIterable(
      Identifier identifier,
      List<BsonDocument> pipeline,
      Class<TModel> model,
      BongoConnectionProvider connectionProvider,
      BongoCodec codec,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool,
      ReadExecutionStrategy<TModel> strategy) {
    this.identifier = identifier;
    this.model = model;
    this.codec = codec;
    this.connectionProvider = connectionProvider;
    this.wireProtocol = wireProtocol;
    this.bufferPool = bufferPool;
    this.pipeline = pipeline;
    this.readExecutionStrategy = strategy;
    this.batchSize = 0;
  }

  public BongoAggregateIterable<TModel> batchSize(@Nullable Integer batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public BongoAggregateIterable<TModel> compress(boolean compress) {
    this.compress = compress;
    return this;
  }

  public BongoAggregateIterable<TModel> cursorType(BongoCursorType cursorType) {
    this.cursorType = cursorType;
    return this;
  }

  public BongoAggregateIterable<TModel> maxTime(final long maxTime, final TimeUnit timeUnit) {
    this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
    return this;
  }

  public List<TModel> into(List<TModel> results) {
    try (var iterator = this.iterator()) {
      while (iterator.hasNext()) {
        results.add(iterator.next());
      }
    }
    return results;
  }

  public void toCollection() {
    var outputIdentifier = getOutIdentifier(pipeline);
    if (outputIdentifier == null) {
      throw new BongoException(
          "toCollection can only be run when the last stage of the aggregation is $out or $merge");
    }

    var aggregateRequestConverter =
        new BongoAggregateRequestConverter(codec.converter(BsonDocument.class));
    var request = new BongoAggregateRequest(identifier, pipeline, batchSize, maxTimeMS);
    try (var cursor =
        new BongoCursor<>(
            readExecutionStrategy.execute(
                identifier,
                aggregateRequestConverter,
                request,
                model,
                batchSize,
                compress,
                cursorType,
                wireProtocol,
                codec,
                bufferPool,
                connectionProvider))) {
      while (cursor.hasNext()) {
        cursor.next();
      }
    }
  }

  public BongoCursor<TModel> iterator() {
    var aggregateRequestConverter =
        new BongoAggregateRequestConverter(codec.converter(BsonDocument.class));
    var outputIdentifier = getOutIdentifier(pipeline);

    var request = new BongoAggregateRequest(identifier, pipeline, batchSize, maxTimeMS);
    if (outputIdentifier == null) {

      return new BongoCursor<>(
          readExecutionStrategy.execute(
              identifier,
              aggregateRequestConverter,
              request,
              model,
              batchSize,
              compress,
              cursorType,
              wireProtocol,
              codec,
              bufferPool,
              connectionProvider));
    } else {
      try (var singleUseStrategy = new ReadExecutionSerialStrategy<TModel>();
          var cursor =
              new BongoCursor<>(
                  singleUseStrategy.execute(
                      identifier,
                      aggregateRequestConverter,
                      request,
                      model,
                      batchSize,
                      compress,
                      cursorType,
                      wireProtocol,
                      codec,
                      bufferPool,
                      connectionProvider))) {
        while (cursor.hasNext()) {
          cursor.next();
        }
      }

      var findRequestConverter = new BongoFindRequestConverter(codec.converter(BsonDocument.class));
      var fo = BongoFindOptions.builder().build();
      var findRequest = new BongoFindRequest(outputIdentifier, fo, new BsonDocument(), batchSize);

      return new BongoCursor<>(
          readExecutionStrategy.execute(
              outputIdentifier,
              findRequestConverter,
              findRequest,
              model,
              batchSize,
              compress,
              cursorType,
              wireProtocol,
              codec,
              bufferPool,
              connectionProvider));
    }
  }

  private @Nullable Identifier getOutIdentifier(List<BsonDocument> pipeline) {
    if (pipeline.size() == 0) {
      return null;
    }

    BsonDocument lastPipelineStage = pipeline.get(pipeline.size() - 1);
    if (lastPipelineStage == null) {
      return null;
    }
    if (lastPipelineStage.containsKey("$out")) {
      if (lastPipelineStage.get("$out").isString()) {
        return new Identifier(
            identifier.getDatabaseName(), lastPipelineStage.getString("$out").getValue());
      } else if (lastPipelineStage.get("$out").isDocument()) {
        BsonDocument outDocument = lastPipelineStage.getDocument("$out");
        if (!outDocument.containsKey("db") || !outDocument.containsKey("coll")) {
          throw new IllegalStateException(
              "Cannot return a cursor when the value for $out stage is not a namespace document");
        }
        return new Identifier(
            outDocument.getString("db").getValue(), outDocument.getString("coll").getValue());
      } else {
        throw new IllegalStateException(
            "Cannot return a cursor when the value for $out stage "
                + "is not a string or namespace document");
      }
    } else if (lastPipelineStage.containsKey("$merge")) {
      if (lastPipelineStage.isString("$merge")) {
        return new Identifier(
            identifier.getDatabaseName(), lastPipelineStage.getString("$merge").getValue());
      } else if (lastPipelineStage.isDocument("$merge")) {
        BsonDocument mergeDocument = lastPipelineStage.getDocument("$merge");
        if (mergeDocument.isDocument("into")) {
          BsonDocument intoDocument = mergeDocument.getDocument("into");
          return new BongoCollection.Identifier(
              intoDocument.getString("db", new BsonString(identifier.getDatabaseName())).getValue(),
              intoDocument.getString("coll").getValue());
        } else if (mergeDocument.isString("into")) {
          return new Identifier(
              identifier.getDatabaseName(), mergeDocument.getString("into").getValue());
        }
      } else {
        throw new IllegalStateException(
            "Cannot return a cursor when the value for $merge stage is not a string or a document");
      }
    }

    return null;
  }
}
