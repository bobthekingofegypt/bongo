package org.bobstuff.bongo.topology;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor()
public class ServerId {
  private ClusterId clusterId;
  private ServerAddress serverAddress;
}
