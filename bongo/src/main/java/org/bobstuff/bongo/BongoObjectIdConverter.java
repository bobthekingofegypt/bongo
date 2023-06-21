package org.bobstuff.bongo;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonType;
import org.bobstuff.bobbson.reader.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoObjectIdConverter implements BobBsonConverter<ObjectId> {
  @Override
  public @Nullable ObjectId readValue(BsonReader bsonReader, BsonType type) {
    return new ObjectId(bsonReader.readObjectId());
  }

  @Override
  public void writeValue(BsonWriter bsonWriter, ObjectId value) {
    byte[] byteValue = value.toByteArray();
    bsonWriter.writeObjectId(byteValue);
  }
}
