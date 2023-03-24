package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bongo.BongoCollection;
import org.bobstuff.bongo.BongoWriteConcern;
import org.bobstuff.bongo.BongoWriteOperationType;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BongoWriteRequest {
  private BongoWriteOperationType type;
  private BongoCollection.Identifier identifier;
  private BongoWriteConcern writeConcern;
  private boolean ordered;
}
