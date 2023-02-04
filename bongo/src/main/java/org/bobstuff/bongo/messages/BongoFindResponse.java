package org.bobstuff.bongo.messages;

import java.util.List;
import lombok.Data;

@Data
public class BongoFindResponse<TModel> {
  private long id;
  private List<TModel> batch;
  private double ok;
  private String errmsg;
  private String codeName;
  private int code;

  public boolean isOk() {
    return ok == 1.0;
  }

  public boolean hasMore() {
    return isOk() && id != 0;
  }
}
