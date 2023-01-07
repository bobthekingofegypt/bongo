package org.bobstuff.bongo;

import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.topology.BongoConnectionProvider;

public class BongoDatabase {
  private String databaseName;
  private BongoConnectionProvider connectionProvider;

  private BongoCodec codec;
  private WireProtocol wireProtocol;
  private BufferDataPool bufferPool;

  public BongoDatabase(
      String databaseName,
      BongoConnectionProvider connectionProvider,
      BongoCodec codec,
      BufferDataPool bufferPool,
      WireProtocol wireProtocol) {
    this.databaseName = databaseName;
    this.connectionProvider = connectionProvider;
    this.wireProtocol = wireProtocol;
    this.codec = codec;
    this.bufferPool = bufferPool;
  }

  public <T> BongoCollection<T> getCollection(String collectionName, Class<T> model) {
    return new BongoCollection<T>(
        new BongoCollection.Identifier(databaseName, collectionName),
        model,
        connectionProvider,
        wireProtocol,
        bufferPool,
        codec);
  }
}
