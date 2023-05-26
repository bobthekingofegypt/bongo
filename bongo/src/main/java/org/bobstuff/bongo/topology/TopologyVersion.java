package org.bobstuff.bongo.topology;

import lombok.Data;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;
import org.bson.types.ObjectId;

@Data
@GenerateBobBsonConverter
public class TopologyVersion {
  private ObjectId processId;
  private long counter;
}
