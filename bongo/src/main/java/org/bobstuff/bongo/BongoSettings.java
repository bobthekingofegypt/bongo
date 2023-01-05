package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocketPoolProvider;

@Builder
@Data
public class BongoSettings {
  private final BongoConnectionSettings connectionSettings;
  private final BongoSocketPoolProvider socketPoolProvider;
  private final BufferDataPool bufferPool;
  private final BongoCodec codec;
}
