package org.bobstuff.bongo.converters;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonType;
import org.bobstuff.bobbson.reader.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.checkerframework.checker.nullness.qual.NonNull;

public class BongoObjectIdConverter implements BobBsonConverter<byte[]> {
  @Override
  public byte[] readValue(BsonReader bsonReader, BsonType type) {
    return bsonReader.readObjectId();
  }

  @Override
  public void writeValue(@NonNull BsonWriter bsonWriter, byte @NonNull [] value) {
    bsonWriter.writeObjectId(value);
  }
}
