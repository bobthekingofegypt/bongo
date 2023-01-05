package org.bobstuff.bongo.auth;

import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.types.Binary;

@CompiledBson
@Data
public class FinalScramMessage {
  private int conversationId;
  private boolean done;
  private Binary payload;
  private double ok;
  private String errmsg;
  private int code;
  private String codeName;

  public boolean isOk() {
    return ok == 1.0;
  }
}
