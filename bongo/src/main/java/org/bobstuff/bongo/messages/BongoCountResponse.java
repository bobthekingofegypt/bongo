package org.bobstuff.bongo.messages;

import lombok.Data;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;

@Data
@GenerateBobBsonConverter
public class BongoCountResponse {
  private double ok;
  private long n;
  private String errmsg;
  private String codeName;
  private int code;

  public boolean isOk() {
    return ok == 1.0;
  }
}
