package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;
import org.checkerframework.checker.nullness.qual.Nullable;

@Data
@Builder
public class BongoUpdateOptions {
  private @Nullable Boolean compress;
  private @Nullable String comment;
  private @Nullable Boolean bypassDocumentValidation;
  private boolean upsert;
}
