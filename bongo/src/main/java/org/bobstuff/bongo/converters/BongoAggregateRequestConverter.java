package org.bobstuff.bongo.converters;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonType;
import org.bobstuff.bobbson.reader.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.messages.BongoAggregateRequest;
import org.bson.BsonDocument;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoAggregateRequestConverter implements BobBsonConverter<BongoAggregateRequest> {
  private BobBsonConverter<BsonDocument> documentConverter;

  public BongoAggregateRequestConverter(BobBsonConverter<BsonDocument> documentConverter) {
    this.documentConverter = documentConverter;
  }

  @Override
  public @Nullable BongoAggregateRequest readValue(
      @UnknownKeyFor @NonNull @Initialized BsonReader bsonReader,
      @UnknownKeyFor @NonNull @Initialized BsonType type) {
    throw new UnsupportedOperationException("method not implemented");
  }

  @Override
  public void write(
      @UnknownKeyFor @NonNull @Initialized BsonWriter bsonWriter,
      @NonNull BongoAggregateRequest value) {
    var identifier = value.getIdentifier();
    var pipeline = value.getPipeline();

    bsonWriter.writeStartDocument();
    bsonWriter.writeString("aggregate", identifier.getCollectionName());
    bsonWriter.writeString("$db", identifier.getDatabaseName());
    bsonWriter.writeStartArray("pipeline");
    for (var pipelineStage : pipeline) {
      documentConverter.write(bsonWriter, pipelineStage);
    }
    bsonWriter.writeEndArray();

    if (value.getMaxTimeMS() > 0) {
      bsonWriter.writeLong("maxTimeMS", value.getMaxTimeMS());
    }
    bsonWriter.writeStartDocument("cursor");
    var batchSize = value.getBatchSize();
    if (batchSize != null && batchSize > 0) {
      bsonWriter.writeInteger("batchSize", batchSize);
    }
    bsonWriter.writeEndDocument();
    bsonWriter.writeBoolean("allowDiskUse", true);

    bsonWriter.writeEndDocument();
  }

  @Override
  public void writeValue(
      @UnknownKeyFor @NonNull @Initialized BsonWriter bsonWriter, BongoAggregateRequest value) {
    throw new UnsupportedOperationException("method not implemented");
  }
}
