package org.bobstuff.bongo.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.BsonWriterOptions;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.checkerframework.checker.nullness.qual.Nullable;

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

  @Nullable
  @BsonWriterOptions(writeNull = false)
  private Integer batchSize;
}
