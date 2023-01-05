package org.bobstuff.bongo.topology;

public enum TopologyType {
  Single,
  ReplicaSetNoPrimary,
  ReplicaSetWithPrimary,
  Sharded,
  LoadBalanced,
  Unknown
}
