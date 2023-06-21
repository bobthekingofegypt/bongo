package org.bobstuff.bongo.messages;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonType;
import org.bobstuff.bobbson.reader.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.BongoWriteConcern;
import org.bobstuff.bongo.exception.BongoException;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoWriteConcernConverter implements BobBsonConverter<BongoWriteConcern> {
  @Override
  public @Nullable BongoWriteConcern readValue(
      @UnknownKeyFor @NonNull @Initialized BsonReader bsonReader,
      @UnknownKeyFor @NonNull @Initialized BsonType type) {
    throw new UnsupportedOperationException("method not implemented");
  }

  @Override
  public void writeValue(
      @UnknownKeyFor @NonNull @Initialized BsonWriter bsonWriter, BongoWriteConcern value) {
    throw new UnsupportedOperationException("method not implemented");
  }

  @Override
  public void write(
      @NonNull BsonWriter bsonWriter, byte @Nullable [] key, @NonNull BongoWriteConcern value) {
    if (key == null) {
      throw new BongoException("WriteConcern cannot be called without a key");
    }
    bsonWriter.writeStartDocument(key);
    var wString = value.getWString();
    if (wString != null) {
      bsonWriter.writeString("w", wString);
    } else if (value.getWNumber() != -1) {
      bsonWriter.writeInteger("w", value.getWNumber());
    }

    var journal = value.getJournal();
    if (journal != null) {
      bsonWriter.writeBoolean("j", journal);
    }

    bsonWriter.writeEndDocument();
  }
}
