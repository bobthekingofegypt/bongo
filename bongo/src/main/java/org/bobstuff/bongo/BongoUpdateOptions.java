package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BongoUpdateOptions {
  private Boolean compress;
  private String comment;
  private Boolean bypassDocumentValidation;
  private boolean upsert;
}
