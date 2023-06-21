package org.bobstuff.bongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bobbson.writer.StackBsonWriter;
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

    var writerId = (BongoBsonWriterId) writer;
    var writtenId = writerId.getWrittenId();
    if (writtenId != null) {
      ids.put(index, writtenId);
      writerId.reset();
    }
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

  public void write(BobBsonBuffer buffer, BongoIndexMap indexMap) {
    var item = items.get(index);
    var operationType = item.getType();

    var writer =
        switch (operationType) {
          case Insert -> new BongoBsonWriterId(buffer);
          default -> new StackBsonWriter(buffer);
        };

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
        indexMap.add(index);
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
      BongoWriteOperationType type,
      ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>> queue) {
    var filteredItems = new ArrayList<BongoBulkWriteOperationIndexedWrapper<TModel>>();
    for (int i = 0; i < items.size(); i++) {
      var item = items.get(i);
      if (item.getType() == type) {
        filteredItems.add(new BongoBulkWriteOperationIndexedWrapper<>(i, item));
      }
    }
    queue.addAll(filteredItems);
  }

  @Override
  public Map<Integer, byte[]> getIds() {
    return ids;
  }
}
