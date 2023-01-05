package org.bobstuff.bongo.topology;

import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ServerDescription {
  private ServerType serverType;
  private ServerAddress address;
  private TopologyVersion topologyVersion;
  private List<ServerAddress> hosts;
  private String primary;

  public boolean isPrimary() {
    return serverType == ServerType.RSPrimary || serverType == ServerType.Standalone;
  }

  public static ServerDescription from(ServerAddress serverAddress, HelloResponse helloResponse) {
    return ServerDescription.builder()
        .serverType(helloResponse.getServerType())
        .address(serverAddress)
        .hosts(
            helloResponse.getHosts() != null
                ? helloResponse.getHosts().stream().map(ServerAddress::new).toList()
                : Collections.emptyList())
        .topologyVersion(helloResponse.getTopologyVersion())
        .build();
  }
}
