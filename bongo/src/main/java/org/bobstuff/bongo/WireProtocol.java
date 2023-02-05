package org.bobstuff.bongo;

import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lombok.ToString;
import org.bobstuff.bobbson.*;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.exception.BongoSocketReadException;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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

  public Response<BobBsonBuffer> readRawServerResponse(BongoSocket socket, boolean decompress) {
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

      if (!decompress) {
        try {
          InputStream bis = new ByteArrayInputStream(messageBufferArray);
          ZstdInputStreamNoFinalizer is = new ZstdInputStreamNoFinalizer(bis);
          byte[] d = new byte[4];
          is.read(d);
          ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
          buffer.put(d);
          buffer.rewind();
          int value = buffer.getInt();
          //        System.out.println("-- " + value);
          responseHeader.setMessageLength(uncompressedSize);
          return new Response<>(responseHeader, value, compressMessageBuffer);
        } catch (RuntimeException e) {
          throw new BongoException("Unexpected exception decompressing with zstd", e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
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
      }
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
    return this.sendCommandMessage(socket, converter, value, compress, stream, null);
  }

  public <T, V> int sendCommandMessage(
      BongoSocket socket,
      BobBsonConverter<T> converter,
      @NonNull T value,
      boolean compress,
      boolean stream,
      @Nullable BongoPayload<V> payload) {
    var requestId = socket.getNextRequestId();
    var buffer =
        prepareCommandMessage(socket, converter, value, compress, stream, payload, requestId);

    for (var buf : buffer.getBuffers()) {
      socket.write(buf);
    }
    buffer.release();

    return requestId;
  }

  public <T, V> DynamicBobBsonBuffer prepareCommandMessage(
      BongoSocket socket,
      BobBsonConverter<T> converter,
      @NonNull T value,
      boolean compress,
      boolean stream,
      @Nullable BongoPayload<V> payload,
      int requestId) {
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

      if (payload != null) {
        contentBuffer.writeByte((byte) 1);
        int position = contentBuffer.getTail();
        contentBuffer.skipTail(4);
        contentBuffer.writeString(payload.getIdentifier());
        contentBuffer.writeByte((byte) 0);

        payload.getItems().write(contentBuffer);

        contentBuffer.writeInteger(position, contentBuffer.getTail() - position);
      }

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
        var outputBuffer = bufferPool.allocate(contentBuffer.getTail());
        var outputBufferArray = outputBuffer.getArray();
        if (outputBufferArray == null) {
          throw new BongoException("Output buffer has inaccessible backing array");
        }
        var position = 0;
        for (var b : contentBuffer.getBuffers()) {
          var bArray = b.getArray();
          if (bArray == null) {
            throw new BongoException("Buffer has inaccessible backing array");
          }
          System.arraycopy(
              bArray, b.getHead(), outputBufferArray, position, b.getTail() - b.getHead());
          position += b.getTail() - b.getHead();
        }
        compressedBuffer =
            compressor.compress(outputBufferArray, 0, contentBuffer.getTail(), bufferPool);

        bufferPool.recycle(outputBuffer);
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

      contentBuffer.release();
    } else {
      var flagBits = 0;
      if (stream) {
        flagBits |= 1 << 16;
      }
      buffer.writeInteger(flagBits); // flag bits
      buffer.writeByte((byte) 0);

      var bsonOutput = new BsonWriter(buffer);
      converter.write(bsonOutput, value);

      if (payload != null) {
        buffer.writeByte((byte) 1);
        int position = buffer.getTail();
        buffer.skipTail(4);
        buffer.writeString(payload.getIdentifier());
        buffer.writeByte((byte) 0);

        payload.getItems().write(buffer);

        buffer.writeInteger(position, buffer.getTail() - position);
      }
    }

    buffer.writeInteger(0, buffer.getTail());

    return buffer;
  }

  @ToString
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
