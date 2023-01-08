package org.bobstuff.bongo;

import org.bobstuff.bobbson.writer.BsonWriter;

public interface BongoInsertProcessor<T> {
  void process(BsonWriter writer, T obj, int index);
}
