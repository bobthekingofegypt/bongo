package org.bobstuff.bongo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BongoInsert<TModel> implements BongoWriteOperation<TModel> {
  private TModel item;

  @Override
  public BongoWriteOperationType getType() {
    return BongoWriteOperationType.Insert;
  }
}
