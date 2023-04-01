package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BongoInsertOneOptions {
  private String comment;
  private Boolean bypassDocumentValidation;
}
