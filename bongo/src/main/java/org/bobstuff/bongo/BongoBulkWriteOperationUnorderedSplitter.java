package org.bobstuff.bongo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.exception.BongoException;

public class BongoBulkWriteOperationUnorderedSplitter<TModel>
    implements BongoBulkOperationSplitter<TModel> {
  private ConcurrentLinkedQueue<BongoWriteOperation<TModel>> items;
  BongoCodec codec;
  private BongoWriteOperationType currentType;

  private Class<TModel> model;
  private Map<Integer, byte[]> ids;

  public BongoBulkWriteOperationUnorderedSplitter(
      ConcurrentLinkedQueue<BongoWriteOperation<TModel>> items,
      Class<TModel> model,
      BongoCodec codec) {
    this.ids = new HashMap<>(items.size());
    this.codec = codec;
    this.model = model;
    this.items = items;
    var item = items.peek();
    if (item == null) {
      throw new BongoException(
          "Items passed to BongoBulkWriteOperationUnorderedSplitter cannot be empty");
    }
    this.currentType = item.getType();
  }

  private void writeUpdate(BsonWriter writer, BongoUpdate<TModel> item) {
    codec.converter(BongoUpdate.class).write(writer, item);
  }

  private void writeInsert(BsonWriter writer, BongoInsert<TModel> insert) {
    var item = insert.getItem();
    if (item == null) {
      throw new BongoException("item cannot be null in an insert request");
    }
    codec.converter(model).write(writer, item);
  }

  private void writeDelete(BsonWriter writer, BongoDelete<TModel> item) {
    codec.converter(BongoDelete.class).write(writer, item);
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
    return currentType;
  }

  public void write(BobBsonBuffer buffer) {
    var writer = new BsonWriter(buffer);
    var item = items.poll();
    while (item != null) {
      var start = buffer.getTail();
      var operationType = item.getType();
      if (operationType != currentType) {
        throw new BongoException("UnorderedSplitter should only contain items of a single type");
      }

      switch (operationType) {
        case Update -> writeUpdate(writer, (BongoUpdate<TModel>) item);
        case Insert -> writeInsert(writer, (BongoInsert<TModel>) item);
        case Delete -> writeDelete(writer, (BongoDelete<TModel>) item);
      }

      var end = buffer.getTail();
      if (end > (16777216 - 42)) {
        buffer.setTail(start);
        // items couldn't be added to the message so place it back into the queue
        items.offer(item);
        break;
      }

      item = items.poll();
      if (item == null) {
        break;
      }
    }
  }

  public boolean hasMore() {
    return !items.isEmpty();
  }

  @Override
  public void drainToQueue(
      BongoWriteOperationType type, ConcurrentLinkedQueue<BongoWriteOperation<TModel>> queue) {
    throw new UnsupportedOperationException("UnorderedSplitter doesn't support drain to queue");
  }
}
