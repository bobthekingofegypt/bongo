package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bobstuff.bongo.BongoCollection;
import org.bobstuff.bongo.BongoFindOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

@Getter
@AllArgsConstructor
public class BongoFindRequest {
  private BongoCollection.Identifier identifier;
  private @Nullable BongoFindOptions findOptions;
}
