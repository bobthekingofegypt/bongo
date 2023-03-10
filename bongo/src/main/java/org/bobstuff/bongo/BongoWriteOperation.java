package org.bobstuff.bongo;

public interface BongoWriteOperation<TModel> {
  BongoWriteOperationType getType();
}
