package org.bobstuff.bongo.compressors;

import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface BongoCompressor {
  byte getId();

  String getIdentifier();

  BobBsonBuffer compress(byte[] data, BobBsonBufferPool bufferPool);

  BobBsonBuffer compress(byte[] data, int offset, int length, BobBsonBufferPool bufferPool);

  void decompress(
      byte[] data, int dataOffset, int dataSize, byte[] dst, int dstOffset, int dstSize);

  static boolean shouldCompress(@Nullable BongoCompressor compressor, @Nullable Boolean compress) {
    if (compressor == null && compress != null && compress) {
      throw new IllegalStateException(
          "Compression requested on call but no compressors registered");
    }
    return compressor != null && (compress == null || compress);
  }
}
