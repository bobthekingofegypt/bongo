package org.bobstuff.bongo;

import java.util.List;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.converters.BongoAggregateRequestConverter;
import org.bobstuff.bongo.executionstrategy.ReadAggregateExecutionSerialStrategy;
import org.bobstuff.bongo.messages.BongoAggregateRequest;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoAggregateIterable<TModel> {
  private BongoCollection.Identifier identifier;
  private Class<TModel> model;
  private @Nullable BongoFindOptions findOptions;
  private @Nullable BsonDocument filter;

  private @Nullable Boolean compress;
  private BongoCursorType cursorType = BongoCursorType.Default;
  private BongoConnectionProvider connectionProvider;
  private BongoCodec codec;

  //  private ReadExecutionStrategy readExecutionStrategy;

  private BufferDataPool bufferPool;
  private WireProtocol wireProtocol;
  private List<BsonDocument> pipeline;

  public BongoAggregateIterable(
      BongoCollection.Identifier identifier,
      List<BsonDocument> pipeline,
      Class<TModel> model,
      BongoConnectionProvider connectionProvider,
      BongoCodec codec,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool) {
    this.identifier = identifier;
    this.model = model;
    this.codec = codec;
    this.connectionProvider = connectionProvider;
    this.wireProtocol = wireProtocol;
    this.bufferPool = bufferPool;
    this.pipeline = pipeline;
    //    this.readExecutionStrategy = readExecutionStrategy;
  }

  public BongoAggregateIterable<TModel> options(BongoFindOptions options) {
    this.findOptions = options;
    return this;
  }

  public BongoAggregateIterable<TModel> filter(BsonDocument filter) {
    this.filter = filter;
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

  public List<TModel> into(List<TModel> results) {
    try (var iterator = this.iterator()) {
      while (iterator.hasNext()) {
        results.add(iterator.next());
      }
    }
    return results;
  }

  public BongoCursor<TModel> iterator() {
    // execute first call, pass results to cursor so it can issue subsequent requests
    var fo = findOptions != null ? findOptions : BongoFindOptions.builder().build();
    var f = filter != null ? filter : new BsonDocument();
    var aggregateRequestConverter =
        new BongoAggregateRequestConverter(codec.converter(BsonDocument.class));
    var request = new BongoAggregateRequest(identifier, null, null, pipeline);
    var executor = new ReadAggregateExecutionSerialStrategy<BongoAggregateRequest, TModel>();
    return new BongoCursor<TModel>(
        executor.execute(
            aggregateRequestConverter,
            request,
            identifier,
            model,
            fo,
            f,
            compress,
            cursorType,
            wireProtocol,
            codec,
            bufferPool,
            connectionProvider));
  }
}
