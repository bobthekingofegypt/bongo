package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.CompiledBson;

@CompiledBson
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BongoInsertRequest {
  @BsonAttribute(value = "insert", order = 1)
  private String insert;

  @BsonAttribute("$db")
  private String db;

  private boolean ordered;
}
