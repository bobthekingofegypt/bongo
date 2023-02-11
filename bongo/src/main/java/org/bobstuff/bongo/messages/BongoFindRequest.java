package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.With;
import org.bobstuff.bongo.BongoCollection;
import org.bobstuff.bongo.BongoFindOptions;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@Getter
@With
@AllArgsConstructor
public class BongoFindRequest {
  private BongoCollection.Identifier identifier;
  private BongoFindOptions findOptions;
  private @Nullable BsonDocument filter;
  private @Nullable Integer batchSize;
}
