package org.bobstuff.bongo.topology;

import lombok.Data;
import org.bson.types.ObjectId;

@Data
public class ClusterId {
  private String key;

  public ClusterId() {
    key = new ObjectId().toHexString();
  }
}
