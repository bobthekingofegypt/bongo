package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@Builder
@With
public class BongoFindOptions {
  private int limit;
  private int skip;
}
