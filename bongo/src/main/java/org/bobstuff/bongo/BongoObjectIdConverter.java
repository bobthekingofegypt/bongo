package org.bobstuff.bongo;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoObjectIdConverter implements BobBsonConverter<ObjectId> {
  @Override
  public ObjectId read(BsonReader bsonReader) {
    return new ObjectId(bsonReader.readObjectId());
  }

  @Override
  public void write(BsonWriter bsonWriter, byte @Nullable [] key, ObjectId value) {
    if (value == null) {
      if (key == null) {
        bsonWriter.writeNull();
      } else {
        bsonWriter.writeNull(key);
      }
    } else {
      byte[] byteValue = value.toByteArray();
      if (key == null) {
        bsonWriter.writeObjectId(byteValue);
      } else {
        bsonWriter.writeObjectId(key, byteValue);
      }
    }
  }

  @Override
  public void write(BsonWriter bsonWriter, ObjectId value) {
    write(bsonWriter, (byte[]) null, value);
  }
}
