package org.bobstuff.bongo;

import java.util.ArrayList;
import java.util.List;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.exception.BongoException;
import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoWrappedBulkItems<TModel> {
  List<TModel> items;
  BobBsonConverter<TModel> converter;
  @Nullable BongoInsertProcessor<TModel> insertProcessor;
  private int index;
  private List<ObjectId> ids;

  public BongoWrappedBulkItems(
      List<TModel> items,
      BobBsonConverter<TModel> converter,
      @Nullable BongoInsertProcessor<TModel> insertProcessor) {
    this.ids = new ArrayList<>(items.size());
    this.items = items;
    this.converter = converter;
    this.insertProcessor = insertProcessor;
    index = 0;
  }

  public List<ObjectId> getIds() {
    return ids;
  }

  public void write(BobBsonBuffer buffer) {
    var writer = new BongoBsonWriterId(buffer);
    for (var i = index; i < items.size(); i += 1) {
      var item = items.get(i);
      if (item == null) {
        throw new BongoException("Cannot write a null document in a bulk write");
      }
      var start = buffer.getTail();

      writer.writeStartDocument();
      if (insertProcessor != null) {
        insertProcessor.process(writer, item, i);
      }
      converter.write(writer, item, false);
      writer.writeEndDocument();

      var writtenId = writer.getWrittenId();
      if (writtenId != null) {
        ids.add(new ObjectId(writtenId));
      }

      var end = buffer.getTail();
      if (end > (16777216 - 42)) {
        buffer.setTail(start);
        break;
      } else {
        index += 1;
        writer.reset();
      }
    }
  }

  public boolean hasMore() {
    return index < items.size();
  }
}
