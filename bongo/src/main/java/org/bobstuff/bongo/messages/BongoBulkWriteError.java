package org.bobstuff.bongo.messages;

import lombok.Data;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;
import org.bson.BsonDocument;

@Data
@GenerateBobBsonConverter
public class BongoBulkWriteError {
  private int index;
  private int code;
  private String errmsg;
  private BsonDocument errInfo;
}
