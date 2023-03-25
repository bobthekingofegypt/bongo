package org.bobstuff.bongo.converters;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoObjectIdConverter implements BobBsonConverter<byte[]> {
  @Override
  public @Nullable byte[] read(BsonReader bsonReader) {
    return bsonReader.readObjectId();
  }

  @Override
  public void write(@NonNull BsonWriter bsonWriter, byte @Nullable [] key, byte @NonNull [] value) {
    if (key == null) {
      bsonWriter.writeObjectId(value);
    } else {
      bsonWriter.writeObjectId(key, value);
    }
  }

  @Override
  public void write(@NonNull BsonWriter bsonWriter, byte @NonNull [] value) {
    bsonWriter.writeObjectId(value);
  }
}
