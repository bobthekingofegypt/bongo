package org.bobstuff.bongo;

import java.io.ByteArrayOutputStream;
import org.bobstuff.bobbson.*;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.exception.BongoSocketReadException;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.checkerframework.checker.nullness.qual.NonNull;

public class WireProtocol {
  private BufferDataPool bufferPool;
  private BongoCodec codec;

  public WireProtocol(BufferDataPool bufferPool, BongoCodec codec) {
    this.bufferPool = bufferPool;
    this.codec = codec;
  }

  public <T, V> Response<T> sendReceiveCommandMessage(
      BongoSocket socket,
      BobBsonConverter<V> requestConverter,
      @NonNull V request,
      BobBsonConverter<T> responseConverter) {
    return this.sendReceiveCommandMessage(
        socket, requestConverter, request, responseConverter, false, false);
  }

  public <T, V> Response<T> sendReceiveCommandMessage(
      BongoSocket socket,
      BobBsonConverter<V> requestConverter,
      @NonNull V request,
      BobBsonConverter<T> responseConverter,
      boolean compress,
      boolean stream) {
    sendCommandMessage(socket, requestConverter, request, compress, stream);
    return readServerResponse(socket, responseConverter);
  }

  public Response<BobBsonBuffer> readRawServerResponse(BongoSocket socket) {
    var responseHeader = socket.readResponseHeader();
    if (responseHeader.getMessageLength() < 0) {
      throw new BongoSocketReadException("Messagelength < 0 in response header");
    }

    BobBsonBuffer messageBuffer;
    if (responseHeader.getOpCode() == 2012) {
      BobBsonBuffer compressionHeaders = socket.read(9);
      int originalOpcode = compressionHeaders.getInt();
      int uncompressedSize = compressionHeaders.getInt();
      int compressorId = compressionHeaders.getByte();
      bufferPool.recycle(compressionHeaders);

      var compressor = socket.getCompressor();
      if (compressor == null) {
        throw new BongoException("Decompression required but no compressor is defined");
      }

      var compressMessageBuffer = socket.read(responseHeader.getMessageLength() - 16 - 9);
      var messageBufferArray = compressMessageBuffer.getArray();
      if (messageBufferArray == null) {
        throw new BongoException("Buffers internal array is not accessible");
      }
      var decompressedMessageBuffer = bufferPool.allocate(uncompressedSize);
      var decompressedMessageBufferArray = decompressedMessageBuffer.getArray();
      if (decompressedMessageBufferArray == null) {
        throw new BongoException("Buffers internal array is not accessible");
      }
      compressor.decompress(
          messageBufferArray,
          compressMessageBuffer.getHead(),
          compressMessageBuffer.getReadRemaining(),
          decompressedMessageBufferArray,
          decompressedMessageBuffer.getHead(),
          uncompressedSize);
      bufferPool.recycle(compressMessageBuffer);
      decompressedMessageBuffer.setTail(uncompressedSize);
      messageBuffer = decompressedMessageBuffer;
    } else {
      messageBuffer = socket.read(responseHeader.getMessageLength() - 16);
    }

    int flagBits = messageBuffer.getInt();
    byte section = messageBuffer.getByte();

    return new Response<>(responseHeader, flagBits, messageBuffer);
  }

  public <T> Response<T> readServerResponse(BongoSocket socket, BobBsonConverter<T> converter) {
    var responseHeader = socket.readResponseHeader();
    if (responseHeader.getMessageLength() < 0) {
      throw new BongoSocketReadException("Messagelength < 0 in response header");
    }

    BobBsonBuffer messageBuffer;
    if (responseHeader.getOpCode() == 2012) {
      BobBsonBuffer compressionHeaders = socket.read(9);
      int originalOpcode = compressionHeaders.getInt();
      int uncompressedSize = compressionHeaders.getInt();
      int compressorId = compressionHeaders.getByte();
      bufferPool.recycle(compressionHeaders);

      var compressor = socket.getCompressor();
      if (compressor == null) {
        throw new BongoException("Decompression required but no compressor is defined");
      }

      var compressMessageBuffer = socket.read(responseHeader.getMessageLength() - 16 - 9);
      var messageBufferArray = compressMessageBuffer.getArray();
      if (messageBufferArray == null) {
        throw new BongoException("Buffers internal array is not accessible");
      }
      var decompressedMessageBuffer = bufferPool.allocate(uncompressedSize);
      var decompressedMessageBufferArray = decompressedMessageBuffer.getArray();
      if (decompressedMessageBufferArray == null) {
        throw new BongoException("Buffers internal array is not accessible");
      }
      compressor.decompress(
          messageBufferArray,
          compressMessageBuffer.getHead(),
          compressMessageBuffer.getReadRemaining(),
          decompressedMessageBufferArray,
          0,
          uncompressedSize);
      bufferPool.recycle(compressMessageBuffer);
      decompressedMessageBuffer.setTail(uncompressedSize);
      messageBuffer = decompressedMessageBuffer;
    } else {
      messageBuffer = socket.read(responseHeader.getMessageLength() - 16);
    }

    int flagBits = messageBuffer.getInt();
    byte section = messageBuffer.getByte();
    var reader = new BsonReader(messageBuffer);
    var response = converter.read(reader);
    if (response == null) {
      throw new BongoSocketReadException(
          "Response was null from the server " + socket.getServerAddress());
    }

    //    messageBuffer.setHead(5);
    //    var readerDebug = new BsonReader(messageBuffer);
    //    var responseDebug = codec.decode(BsonDocument.class, readerDebug);
    //    if (responseDebug != null) {
    //      System.out.println(responseDebug);
    //    }

    bufferPool.recycle(messageBuffer);
    return new Response<>(responseHeader, flagBits, response);
  }

  public <T> int sendCommandMessage(
      BongoSocket socket, BobBsonConverter<T> converter, @NonNull T value) {
    return this.sendCommandMessage(socket, converter, value, false, false);
  }

  public <T> int sendCommandMessage(
      BongoSocket socket,
      BobBsonConverter<T> converter,
      @NonNull T value,
      boolean compress,
      boolean stream) {
    var requestId = socket.getNextRequestId();
    var buffer = new DynamicBobBsonBuffer(bufferPool);
    buffer.skipTail(4);
    buffer.writeInteger(requestId);
    buffer.writeInteger(0);
    buffer.writeInteger(compress ? 2012 : 2013);

    if (compress) {
      var compressor = socket.getCompressor();
      if (compressor == null) {
        throw new BongoException("Compression requested but no compressors are configured");
      }
      var contentBuffer = new DynamicBobBsonBuffer(bufferPool);

      var flagBits = 0;
      if (stream) {
        flagBits |= 1 << 16;
      }
      contentBuffer.writeInteger(flagBits); // flag bits
      contentBuffer.writeByte((byte) 0);

      var bsonOutput = new BsonWriter(contentBuffer);
      converter.write(bsonOutput, value);

      //      contentBuffer.writeInteger(0, contentBuffer.getTail());

      BobBsonBuffer compressedBuffer;
      if (contentBuffer.getBuffers().size() == 1) {
        var innerBuffer = contentBuffer.getBuffers().get(0);
        var innerArray = innerBuffer.getArray();
        if (innerArray == null) {
          throw new BongoException("Dynamic buffers inner buffer has inaccessible array");
        }
        compressedBuffer =
            compressor.compress(
                innerArray, innerBuffer.getHead(), innerBuffer.getTail(), bufferPool);
      } else {
        var outputBuffer = new ByteArrayOutputStream(contentBuffer.getTail());
        for (var b : contentBuffer.getBuffers()) {
          var bArray = b.getArray();
          if (bArray == null) {
            throw new BongoException("Buffer backing array is inaccessible");
          }
          outputBuffer.write(bArray, b.getHead(), b.getTail());
        }
        compressedBuffer = compressor.compress(outputBuffer.toByteArray(), bufferPool);
      }

      buffer.writeInteger(2013);
      buffer.writeInteger(contentBuffer.getTail()); // uncompressed size
      buffer.writeByte(compressor.getId());

      var compressedBufferArray = compressedBuffer.getArray();
      if (compressedBufferArray == null) {
        throw new BongoException("Compressed buffer has inaccesible array");
      }

      buffer.writeBytes(
          compressedBufferArray, compressedBuffer.getHead(), compressedBuffer.getTail());
      bufferPool.recycle(compressedBuffer);
    } else {
      var flagBits = 0;
      if (stream) {
        flagBits |= 1 << 16;
      }
      buffer.writeInteger(flagBits); // flag bits
      buffer.writeByte((byte) 0);

      var bsonOutput = new BsonWriter(buffer);
      converter.write(bsonOutput, value);
    }

    buffer.writeInteger(0, buffer.getTail());

    for (var buf : buffer.getBuffers()) {
      socket.write(buf);
    }
    buffer.release();

    return requestId;
  }

  public static class Response<T> {
    private BongoResponseHeader header;
    private int flagBits;
    private T payload;

    public Response(BongoResponseHeader header, int flagBits, T payload) {
      this.header = header;
      this.flagBits = flagBits;
      this.payload = payload;
    }

    public BongoResponseHeader getHeader() {
      return header;
    }

    public int getFlagBits() {
      return flagBits;
    }

    public T getPayload() {
      return payload;
    }
  }
}
