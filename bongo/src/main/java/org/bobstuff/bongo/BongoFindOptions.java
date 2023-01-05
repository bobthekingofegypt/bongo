package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BongoFindOptions {
  private int limit;
  private int skip;
}
