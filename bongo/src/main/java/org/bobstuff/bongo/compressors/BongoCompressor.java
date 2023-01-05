package org.bobstuff.bongo.compressors;

public interface BongoCompressor {
  int getId();

  void compress(byte[] data);

  byte[] decompress(byte[] data);
}
