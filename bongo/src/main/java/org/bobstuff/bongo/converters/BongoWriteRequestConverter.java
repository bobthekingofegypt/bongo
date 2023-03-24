package org.bobstuff.bongo.converters;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.messages.BongoWriteRequest;
import org.checkerframework.checker.nullness.qual.NonNull;

public class BongoWriteRequestConverter implements BobBsonConverter<BongoWriteRequest> {
  @Override
  public void write(@NonNull BsonWriter bsonWriter, @NonNull BongoWriteRequest value) {
    var identifier = value.getIdentifier();
    var db = identifier.getDatabaseName();
    var col = identifier.getCollectionName();
    var type = value.getType();
    var ordered = value.isOrdered();

    bsonWriter.writeStartDocument();
    bsonWriter.writeString(type.getCommand(), col);
    bsonWriter.writeString("$db", db);

    if (ordered) {
      bsonWriter.writeBoolean("ordered", true);
    }

    bsonWriter.writeEndDocument();
  }
}
