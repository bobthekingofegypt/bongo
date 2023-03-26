package org.bobstuff.bongo.compressors;

import com.github.luben.zstd.Zstd;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.exception.BongoException;

public class BongoCompressorZstd implements BongoCompressor {
  private int compressionLevel;

  public BongoCompressorZstd() {
    this(Zstd.maxCompressionLevel());
  }

  public BongoCompressorZstd(int level) {
    compressionLevel = level;
  }

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
    var outBuffer = bufferPool.allocate((int) Zstd.compressBound(length - offset));
    byte[] out = outBuffer.getArray();
    if (out == null) {
      throw new IllegalStateException(
          "Compression out buffer array is null, "
              + outBuffer.getClass()
              + " must implement getArray()");
    }

    try {
      int compressedSize =
          (int) Zstd.compressByteArray(out, 0, out.length, data, offset, length, compressionLevel);
      outBuffer.setTail(compressedSize);
    } catch (RuntimeException e) {
      throw new BongoException("Unexpected exception compressing with zstd", e);
    }

    return outBuffer;
  }

  @Override
  public void decompress(
      byte[] data, int dataOffset, int dataSize, byte[] dst, int dstOffset, int dstSize) {
    try {
      Zstd.decompressByteArray(dst, dstOffset, dstSize, data, dataOffset, dataSize);
    } catch (RuntimeException e) {
      throw new BongoException("Unexpected exception decompressing with zstd", e);
    }
  }
}
