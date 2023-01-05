package org.bobstuff.bongo.topology;

import java.util.List;
import lombok.Data;
import org.bobstuff.bobbson.annotations.CompiledBson;

@CompiledBson
@Data
public class HelloResponse {
  public static final double OK_RESPONSE = 1.0;
  private boolean helloOk;
  private SpeculativeAuthenticate speculativeAuthenticate;
  private boolean isWritablePrimary;
  private TopologyVersion topologyVersion;
  private int maxBsonObjectSize;
  private int maxMessageSizeBytes;
  private int maxWriteBatchSize;
  // TODO need a converter for this
  //    private long localTime;
  private int logicalSessionTimeoutMinutes;
  private int connectionId;
  private int minWireVersion;
  private int maxWireVersion;
  private boolean readOnly;
  private List<String> hosts;
  private boolean secondary;
  private String setName;
  private double ok;
  private String errmsg;
  private int code;
  private String codeName;

  public boolean isOk() {
    return ok == 1.0;
  }

  public ServerType getServerType() {
    if (ok != OK_RESPONSE) {
      return ServerType.Unknown;
    }

    if (setName != null) {
      if (isWritablePrimary) {
        return ServerType.RSPrimary;
      } else if (secondary) {
        return ServerType.RSSecondary;
      }
    }
    //
    //        if (isReplicaSetMember(helloResult)) {
    //
    //            if (helloResult.getBoolean("hidden", BsonBoolean.FALSE).getValue()) {
    //                return REPLICA_SET_OTHER;
    //            }
    //
    //            if (helloResult.getBoolean("isWritablePrimary", BsonBoolean.FALSE).getValue()) {
    //                return REPLICA_SET_PRIMARY;
    //            }
    //
    //            if (helloResult.getBoolean(LEGACY_HELLO_LOWER, BsonBoolean.FALSE).getValue()) {
    //                return REPLICA_SET_PRIMARY;
    //            }
    //
    //            if (helloResult.getBoolean("secondary", BsonBoolean.FALSE).getValue()) {
    //                return REPLICA_SET_SECONDARY;
    //            }
    //
    //            if (helloResult.getBoolean("arbiterOnly", BsonBoolean.FALSE).getValue()) {
    //                return REPLICA_SET_ARBITER;
    //            }
    //
    //            if (helloResult.containsKey("setName") && helloResult.containsKey("hosts")) {
    //                return com.mongodb.connection.ServerType.REPLICA_SET_OTHER;
    //            }
    //
    //            return ServerType.REPLICA_SET_GHOST;
    //        }

    //        if (helloResult.containsKey("msg") && helloResult.get("msg").equals(new BsonString
    //        ("isdbgrid"))) {
    //            return SHARD_ROUTER;
    //        }

    return ServerType.Standalone;
  }
}
