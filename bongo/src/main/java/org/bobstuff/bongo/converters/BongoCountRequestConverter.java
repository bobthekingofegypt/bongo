package org.bobstuff.bongo.converters;

import java.nio.charset.StandardCharsets;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bson.BsonDocument;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoCountRequestConverter implements BobBsonConverter<BongoFindRequest> {
  private BobBsonConverter<BsonDocument> documentConverter;

  public BongoCountRequestConverter(BobBsonConverter<BsonDocument> documentConverter) {
    this.documentConverter = documentConverter;
  }

  @Override
  public void write(
      @UnknownKeyFor @NonNull @Initialized BsonWriter bsonWriter, @NonNull BongoFindRequest value) {
    var identifier = value.getIdentifier();
    var findOptions = value.getFindOptions();
    var filter = value.getFilter();

    bsonWriter.writeStartDocument();
    bsonWriter.writeString("count", identifier.getCollectionName());
    bsonWriter.writeString("$db", identifier.getDatabaseName());

    if (findOptions.getLimit() > 0) {
      bsonWriter.writeInteger("limit", findOptions.getLimit());
    }
    if (findOptions.getSkip() > 0) {
      bsonWriter.writeInteger("skip", findOptions.getSkip());
    }
    var batchSize = value.getBatchSize();
    if (batchSize != null) {
      bsonWriter.writeInteger("batchSize", batchSize);
    }

    if (filter != null && filter.size() > 0) {
      documentConverter.write(bsonWriter, "filter".getBytes(StandardCharsets.UTF_8), filter);
    }

    bsonWriter.writeEndDocument();
  }
}
