package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BongoPayload {
  private String identifier;
  private BongoBulkOperationSplitter items;
  @Builder.Default private BongoIndexMap indexMap = new BongoIndexMap();
}
