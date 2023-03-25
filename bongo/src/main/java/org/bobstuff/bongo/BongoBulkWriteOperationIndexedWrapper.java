package org.bobstuff.bongo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BongoBulkWriteOperationIndexedWrapper<TModel> {
  private int index;
  private BongoWriteOperation<TModel> operation;
}
