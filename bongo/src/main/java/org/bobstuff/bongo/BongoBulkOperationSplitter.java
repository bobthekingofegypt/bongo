package org.bobstuff.bongo;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bongo.codec.BongoCodec;

public interface BongoBulkOperationSplitter<TModel> {
  BongoCodec getCodec();

  Class<TModel> getModel();

  public BongoWriteOperationType nextType();

  void write(BobBsonBuffer buffer);

  boolean hasMore();

  void drainToQueue(
      BongoWriteOperationType type, ConcurrentLinkedQueue<BongoWriteOperation<TModel>> queue);
}
