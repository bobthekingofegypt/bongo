package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bobstuff.bongo.BongoWriteConcern;

@CompiledBson
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BongoUpdateRequest {
  @BsonAttribute(value = "update", order = 1)
  private String update;

  @BsonAttribute("$db")
  private String db;

  private BongoWriteConcern writeConcern;

  private boolean ordered;
}
