package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Data;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocketPoolProvider;
import org.bobstuff.bongo.monitoring.WireProtocolMonitor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Builder
@Data
public class BongoSettings {
  private final BongoConnectionSettings connectionSettings;
  private final BongoSocketPoolProvider socketPoolProvider;
  private final BobBsonBufferPool bufferPool;
  private final BongoCodec codec;
  private final @Nullable WireProtocolMonitor wireProtocolMonitor;
}
