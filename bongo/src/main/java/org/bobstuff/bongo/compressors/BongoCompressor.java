package org.bobstuff.bongo.compressors;

import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;

public interface BongoCompressor {
  byte getId();

  String getIdentifier();

  BobBsonBuffer compress(byte[] data, BufferDataPool bufferPool);

  BobBsonBuffer compress(byte[] data, int offset, int length, BufferDataPool bufferPool);

  void decompress(
      byte[] data, int dataOffset, int dataSize, byte[] dst, int dstOffset, int dstSize);
}
