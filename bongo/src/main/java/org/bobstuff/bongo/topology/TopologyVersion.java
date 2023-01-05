package org.bobstuff.bongo.topology;

import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.types.ObjectId;

@Data
@CompiledBson
public class TopologyVersion {
  private ObjectId processId;
  private long counter;
}
