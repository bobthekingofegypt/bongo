package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BongoPayload<TModel> {
  private String identifier;
  private Class<TModel> model;
  private BongoWrappedBulkItems<TModel> items;
}
