package org.bobstuff.bongo.compressors;

import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface BongoCompressor {
  byte getId();

  String getIdentifier();

  BobBsonBuffer compress(byte[] data, BufferDataPool bufferPool);

  BobBsonBuffer compress(byte[] data, int offset, int length, BufferDataPool bufferPool);

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
