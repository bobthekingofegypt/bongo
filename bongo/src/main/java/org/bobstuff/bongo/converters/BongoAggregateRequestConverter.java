package org.bobstuff.bongo.converters;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.messages.BongoAggregateRequest;
import org.bson.BsonDocument;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoAggregateRequestConverter implements BobBsonConverter<BongoAggregateRequest> {
  private BobBsonConverter<BsonDocument> documentConverter;

  public BongoAggregateRequestConverter(BobBsonConverter<BsonDocument> documentConverter) {
    this.documentConverter = documentConverter;
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

    bsonWriter.writeStartDocument("cursor");
    bsonWriter.writeEndDocument();
    bsonWriter.writeBoolean("allowDiskUse", true);
    //    if (findOptions != null) {
    //      if (findOptions.getLimit() > 0) {
    //        bsonWriter.writeInteger("limit", findOptions.getLimit());
    //      }
    //      if (findOptions.getSkip() > 0) {
    //        bsonWriter.writeInteger("skip", findOptions.getSkip());
    //      }
    //      if (findOptions.getBatchSize() != null) {
    //        bsonWriter.writeInteger("batchSize", findOptions.getBatchSize());
    //      }
    //    }

    //    if (filter != null && filter.size() > 0) {
    //      documentConverter.write(bsonWriter, "filter".getBytes(StandardCharsets.UTF_8), filter);
    //    }

    bsonWriter.writeEndDocument();
  }
}
