package org.bobstuff.bongo.models;

import java.time.Instant;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class InstantConverter implements BobBsonConverter<Instant> {
  @Override
  public @Nullable Instant read(@UnknownKeyFor @NonNull @Initialized BsonReader bsonReader) {
    return Instant.ofEpochMilli(bsonReader.readDateTime());
  }

  @Override
  public void write(@NonNull BsonWriter bsonWriter, byte @Nullable [] key, @NonNull Instant value) {
    if (value == null) {
      value = Instant.now();
    }
    if (key == null) {
      bsonWriter.writeDateTime(value.getEpochSecond());
    } else {
      bsonWriter.writeDateTime(key, value.getEpochSecond());
    }
  }

  @Override
  public void write(@NonNull BsonWriter bsonWriter, @NonNull Instant value) {
    bsonWriter.writeDateTime(value.getEpochSecond());
  }
}
