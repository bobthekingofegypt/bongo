package org.bobstuff.bongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.exception.BongoException;

public class BongoBulkWriteOperationSplitter<TModel> implements BongoBulkOperationSplitter<TModel> {
  private List<? extends BongoWriteOperation<TModel>> items;

  private BongoCodec codec;

  private Class<TModel> model;
  private int index;
  private Map<Integer, byte[]> ids;

  public BongoBulkWriteOperationSplitter(
      List<? extends BongoWriteOperation<TModel>> items, Class<TModel> model, BongoCodec codec) {
    this.ids = new HashMap<>(items.size());
    this.codec = codec;
    this.model = model;
    this.items = items;
    index = 0;
  }

  private void writeUpdate(BsonWriter writer, BongoUpdate<TModel> item) {
    codec.converter(BongoUpdate.class).write(writer, item);
  }

  private void writeDelete(BsonWriter writer, BongoDelete<TModel> item) {
    codec.converter(BongoDelete.class).write(writer, item);
  }

  private void writeInsert(BsonWriter writer, BongoInsert<TModel> insert) {
    var item = insert.getItem();
    if (item == null) {
      throw new BongoException("item cannot be null in an insert request");
    }
    codec.converter(model).write(writer, item);
  }

  @Override
  public BongoCodec getCodec() {
    return codec;
  }

  @Override
  public Class<TModel> getModel() {
    return model;
  }

  public BongoWriteOperationType nextType() {
    return items.get(index).getType();
  }

  public void write(BobBsonBuffer buffer) {
    var item = items.get(index);
    var operationType = item.getType();

    var writer = new BsonWriter(buffer);
    do {
      var start = buffer.getTail();

      switch (operationType) {
        case Update -> writeUpdate(writer, (BongoUpdate<TModel>) item);
        case Insert -> writeInsert(writer, (BongoInsert<TModel>) item);
        case Delete -> writeDelete(writer, (BongoDelete<TModel>) item);
      }

      var end = buffer.getTail();
      if (end > (16777216 - 42)) {
        buffer.setTail(start);
        break;
      } else {
        index += 1;
      }
      if (index == items.size()) {
        break;
      }
      item = items.get(index);
    } while (item.getType() == operationType);
  }

  public boolean hasMore() {
    return index < items.size();
  }

  @Override
  public void drainToQueue(
      BongoWriteOperationType type, ConcurrentLinkedQueue<BongoWriteOperation<TModel>> queue) {
    var filteredItems = items.stream().filter(item -> item.getType() == type).toList();
    queue.addAll(filteredItems);
  }
}
