package org.bobstuff.bongo.messages;

import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.BsonDocument;

@Data
@CompiledBson
public class BongoBulkWriteError {
  private int index;
  private int code;
  private String errmsg;
  private BsonDocument errInfo;
}
