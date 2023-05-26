package org.bobstuff.bongo.executionstrategy;

import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.topology.BongoConnectionProvider;

public interface WriteExecutionStrategy<TModel> extends AutoCloseable {
  BongoBulkWriteResult execute(
      BongoCollection.Identifier identifier,
      BongoBulkOperationSplitter<TModel> splitter,
      BongoBulkWriteOptions options,
      BobBsonBufferPool bufferPool,
      BongoCodec codec,
      BongoConnectionProvider connectionProvider,
      WireProtocol wireProtocol,
      BongoWriteConcern writeConcern);

  boolean isClosed();

  void close();
}
