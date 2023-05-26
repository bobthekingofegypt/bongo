package org.bobstuff.bongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bongo.exception.BongoException;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoWrappedBulkItems<TModel> {
  List<TModel> items;
  BobBsonConverter<TModel> converter;
  @Nullable BongoInsertProcessor<TModel> insertProcessor;
  private int index;
  private int indexOffset;
  private Map<Integer, byte[]> ids;

  public BongoWrappedBulkItems(
      List<TModel> items,
      BobBsonConverter<TModel> converter,
      @Nullable BongoInsertProcessor<TModel> insertProcessor,
      int indexOffset) {
    this.ids = new HashMap<>(items.size());
    this.items = items;
    this.converter = converter;
    this.insertProcessor = insertProcessor;
    index = 0;
    this.indexOffset = indexOffset;
  }

  public Map<Integer, byte[]> getIds() {
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
        ids.put(i + indexOffset, writtenId);
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
