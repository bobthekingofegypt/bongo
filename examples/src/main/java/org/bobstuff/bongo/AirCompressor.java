package org.bobstuff.bongo;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.compressors.BongoCompressor;

public class AirCompressor implements BongoCompressor {
  @Override
  public byte getId() {
    return 3;
  }

  @Override
  public String getIdentifier() {
    return "zstd";
  }

  @Override
  public BobBsonBuffer compress(byte[] data, BufferDataPool bufferPool) {
    return compress(data, 0, data.length, bufferPool);
  }

  @Override
  public BobBsonBuffer compress(byte[] data, int offset, int length, BufferDataPool bufferPool) {
    ZstdCompressor compressor = new ZstdCompressor();
    var outBuffer = bufferPool.allocate((int) compressor.maxCompressedLength(length - offset));
    byte[] out = outBuffer.getArray();
    if (out == null) {
      throw new IllegalStateException(
          "Compression out buffer array is null, "
              + outBuffer.getClass()
              + " must implement getArray()");
    }

    int compressedSize = compressor.compress(data, offset, length, out, 0, out.length);
    outBuffer.setTail(compressedSize);
    return outBuffer;
  }

  @Override
  public void decompress(
      byte[] data, int dataOffset, int dataSize, byte[] dst, int dstOffset, int dstSize) {
    var decompressor = new ZstdDecompressor();
    decompressor.decompress(data, dataOffset, dataSize, dst, dstOffset, dstSize);
  }
}
