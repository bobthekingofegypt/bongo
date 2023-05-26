package org.bobstuff.bongo.topology;

import lombok.Data;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;
import org.bson.types.Binary;

@GenerateBobBsonConverter
@Data
public class SpeculativeAuthenticate {
  private Binary payload;
  private int conversationId;
}
