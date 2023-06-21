package org.bobstuff.bongo.converters;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonType;
import org.bobstuff.bobbson.reader.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.messages.BongoWriteRequest;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoWriteRequestConverter implements BobBsonConverter<BongoWriteRequest> {
  @Override
  public @Nullable BongoWriteRequest readValue(
      @UnknownKeyFor @NonNull @Initialized BsonReader bsonReader,
      @UnknownKeyFor @NonNull @Initialized BsonType type) {
    throw new UnsupportedOperationException("method not implemented");
  }

  @Override
  public void write(@NonNull BsonWriter bsonWriter, @NonNull BongoWriteRequest value) {
    var identifier = value.getIdentifier();
    var db = identifier.getDatabaseName();
    var col = identifier.getCollectionName();
    var type = value.getType();
    var ordered = value.isOrdered();
    var comment = value.getComment();

    bsonWriter.writeStartDocument();
    bsonWriter.writeString(type.getCommand(), col);
    bsonWriter.writeString("$db", db);

    // TODO should I make ordered a Boolean not a primitive?
    bsonWriter.writeBoolean("ordered", ordered);

    if (comment != null) {
      bsonWriter.writeString("comment", comment);
    }

    bsonWriter.writeEndDocument();
  }

  @Override
  public void writeValue(
      @UnknownKeyFor @NonNull @Initialized BsonWriter bsonWriter, BongoWriteRequest value) {
    throw new UnsupportedOperationException("method not implemented");
  }
}
