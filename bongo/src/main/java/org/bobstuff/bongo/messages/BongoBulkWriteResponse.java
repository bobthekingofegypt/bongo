package org.bobstuff.bongo.messages;

import java.util.List;
import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;

@Data
@CompiledBson
public class BongoBulkWriteResponse {
  private double ok;
  private int n;
  private String codeName;
  private String errmsg;
  private int code;
  private List<BongoBulkWriteError> writeErrors;
}
