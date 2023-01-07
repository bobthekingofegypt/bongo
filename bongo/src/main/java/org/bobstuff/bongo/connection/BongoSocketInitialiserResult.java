package org.bobstuff.bongo.connection;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.topology.ServerDescription;

@Data
@AllArgsConstructor
public class BongoSocketInitialiserResult {
  private ServerDescription serverDescription;
  private BongoCompressor compressor;
}
