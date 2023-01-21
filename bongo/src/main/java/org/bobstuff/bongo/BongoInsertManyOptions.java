package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BongoInsertManyOptions {
  private @Builder.Default boolean ordered = true;
  private Boolean bypassDocumentValidation;
  private Boolean compress;
}
