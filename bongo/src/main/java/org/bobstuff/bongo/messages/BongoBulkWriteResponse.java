package org.bobstuff.bongo.messages;

import java.util.List;
import lombok.Data;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;

@Data
@GenerateBobBsonConverter
public class BongoBulkWriteResponse {
  private double ok;
  private int n;
  private String codeName;
  private String errmsg;
  private int code;
  private List<BongoBulkWriteError> writeErrors;
  @BsonAttribute("nModified")
  private int numModified;
  private List<BongoIndexedIdOperation> upserted;
}
