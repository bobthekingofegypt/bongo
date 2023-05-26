package org.bobstuff.bongo;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bongo.codec.BongoCodec;

public interface BongoBulkOperationSplitter<TModel> {
  BongoCodec getCodec();

  Class<TModel> getModel();

  public BongoWriteOperationType nextType();

  void write(BobBsonBuffer buffer, BongoIndexMap indexMap);

  boolean hasMore();

  void drainToQueue(
      BongoWriteOperationType type,
      ConcurrentLinkedQueue<BongoBulkWriteOperationIndexedWrapper<TModel>> queue);

  Map<Integer, byte[]> getIds();
}
