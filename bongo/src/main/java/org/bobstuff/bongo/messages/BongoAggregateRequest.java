package org.bobstuff.bongo.messages;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bobstuff.bongo.BongoCollection;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@Getter
@AllArgsConstructor
public class BongoAggregateRequest {
  private BongoCollection.Identifier identifier;
  private List<BsonDocument> pipeline;
  private @Nullable Integer batchSize;
  private long maxTimeMS;
}
