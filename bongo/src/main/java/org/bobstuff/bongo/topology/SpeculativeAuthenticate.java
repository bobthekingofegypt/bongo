package org.bobstuff.bongo.topology;

import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.types.Binary;

@CompiledBson
@Data
public class SpeculativeAuthenticate {
  private Binary payload;
  private int conversationId;
}
