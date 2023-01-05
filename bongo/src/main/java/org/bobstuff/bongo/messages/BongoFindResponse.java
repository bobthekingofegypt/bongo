package org.bobstuff.bongo.messages;

import java.util.List;
import lombok.Data;

@Data
public class BongoFindResponse<TModel> {
  private int flags;
  private long id;
  private List<TModel> batch;
  private double ok;
  private String errmsg;
}
