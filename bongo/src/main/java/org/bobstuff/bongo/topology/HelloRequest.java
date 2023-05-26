package org.bobstuff.bongo.topology;

import lombok.Data;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;

@GenerateBobBsonConverter
@Data
public class HelloRequest {
  @BsonAttribute(value = "hello", order = 1)
  private int hello;

  private boolean helloOk;

  @BsonAttribute("$db")
  private String db;

  private TopologyVersion topologyVersion;

  private long maxAwaitTimeMS;

  public static HelloRequest create(ServerDescription serverDescription) {
    var request = new HelloRequest();
    request.setHello(1);
    request.setHelloOk(true);
    request.setDb("admin");

    if (serverDescription != null) {
      request.setTopologyVersion(serverDescription.getTopologyVersion());
      request.setMaxAwaitTimeMS(5000);
    }

    return request;
  }
}
