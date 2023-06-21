package org.bobstuff.bongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.writer.StackBsonWriter;
import org.bobstuff.bongo.exception.BongoException;

public class BongoUpdateWrappedBulkItems<TModel> extends BongoWrappedBulkItems<TModel> {
  List<TModel> items;
  BobBsonConverter<TModel> converter;
  private int index;
  private int indexOffset;
  private Map<Integer, byte[]> ids;

  public BongoUpdateWrappedBulkItems(
      List<TModel> items, BobBsonConverter<TModel> converter, int indexOffset) {
    super(items, converter, null, indexOffset);
    this.ids = new HashMap<>(items.size());
    this.items = items;
    this.converter = converter;
    index = 0;
    this.indexOffset = indexOffset;
  }

  public Map<Integer, byte[]> getIds() {
    return ids;
  }

  public void write(BobBsonBuffer buffer) {
    var writer = new StackBsonWriter(buffer);
    for (var i = index; i < items.size(); i += 1) {
      var item = items.get(i);
      if (item == null) {
        throw new BongoException("Cannot write a null document in a bulk write");
      }
      var start = buffer.getTail();

      writer.writeStartDocument();
      // TODO DELETE THIS?
      converter.write(writer, item);
      writer.writeEndDocument();

      var end = buffer.getTail();
      if (end > (16777216 - 42)) {
        buffer.setTail(start);
        break;
      } else {
        index += 1;
      }
    }
  }

  public boolean hasMore() {
    return index < items.size();
  }
}
