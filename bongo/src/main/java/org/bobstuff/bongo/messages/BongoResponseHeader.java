package org.bobstuff.bongo.messages;

import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;

@Data
@CompiledBson
public class BongoResponseHeader {
  private int messageLength;
  private int requestId;
  private int responseTo;
  private int opCode;
}
