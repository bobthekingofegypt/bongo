package org.bobstuff.bongo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bobbson.writer.StackBsonWriter;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.exception.BongoException;

public class BongoBulkWriteOperationUnorderedSplitter<TModel>
    implements BongoBulkOperationSplitter<TModel> {
  private ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>> items;
  BongoCodec codec;
  private BongoWriteOperationType currentType;

  private Class<TModel> model;
  private Map<Integer, byte[]> ids;

  public BongoBulkWriteOperationUnorderedSplitter(
      ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>> items,
      Class<TModel> model,
      BongoCodec codec) {
    this.ids = new ConcurrentHashMap<>(items.size());
    this.codec = codec;
    this.model = model;
    this.items = items;
    var item = items.peek();
    if (item == null) {
      throw new BongoException(
          "Items passed to BongoBulkWriteOperationUnorderedSplitter cannot be empty");
    }
    this.currentType = item.getOperation().getType();
  }

  private void writeUpdate(BsonWriter writer, BongoUpdate<TModel> item) {
    codec.converter(BongoUpdate.class).write(writer, item);
  }

  private void writeInsert(BsonWriter writer, BongoInsert<TModel> insert, int index) {
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

  public void write(BobBsonBuffer buffer, BongoIndexMap indexMap) {
    var writer =
        switch (currentType) {
          case Insert -> new BongoBsonWriterId(buffer);
          default -> new StackBsonWriter(buffer);
        };
    var item = items.poll();
    while (item != null) {
      var start = buffer.getTail();
      var operation = item.getOperation();
      var operationType = operation.getType();
      if (operationType != currentType) {
        throw new BongoException("UnorderedSplitter should only contain items of a single type");
      }

      switch (operationType) {
        case Update -> writeUpdate(writer, (BongoUpdate<TModel>) operation);
        case Insert -> writeInsert(writer, (BongoInsert<TModel>) operation, item.getIndex());
        case Delete -> writeDelete(writer, (BongoDelete<TModel>) operation);
      }

      var end = buffer.getTail();
      if (end > (16777216 - 42)) {
        buffer.setTail(start);
        // items couldn't be added to the message so place it back into the queue
        items.offer(item);
        break;
      }

      indexMap.add(item.getIndex());
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
      BongoWriteOperationType type,
      ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>> queue) {
    throw new UnsupportedOperationException("UnorderedSplitter doesn't support drain to queue");
  }

  @Override
  public Map<Integer, byte[]> getIds() {
    return ids;
  }
}
