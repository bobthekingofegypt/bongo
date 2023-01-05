package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.CompiledBson;

@CompiledBson
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BongoGetMoreRequest {
  @BsonAttribute(value = "getMore", order = 1)
  private long more;

  @BsonAttribute("$db")
  private String db;

  private String collection;
}
