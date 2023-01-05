package org.bobstuff.bongo.topology;

public enum ServerType {
  Standalone,
  Mongos,
  PossiblePrimary,
  RSPrimary,
  RSSecondary,
  RSArbiter,
  RSOther,
  RSGhost,
  LoadBalancer,
  Unknown;
}
