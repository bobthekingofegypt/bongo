package org.bobstuff.bongo;

import org.bobstuff.bobbson.*;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.buffer.DynamicBobBsonBuffer;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bobbson.writer.StackBsonWriter;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.connection.BongoRequestIDGenerator;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.exception.BongoSocketReadException;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.bobstuff.bongo.monitoring.WireProtocolMonitor;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WireProtocol {
  private final BobBsonBufferPool bufferPool;

  private final BongoRequestIDGenerator requestIDGenerator;

  private final @MonotonicNonNull WireProtocolMonitor monitor;

  public WireProtocol(
      BobBsonBufferPool bufferPool, BongoRequestIDGenerator requestIDGenerator) {
    this(bufferPool, requestIDGenerator, null);
  }

  public WireProtocol(
      BobBsonBufferPool bufferPool,
      BongoRequestIDGenerator requestIDGenerator,
      @Nullable WireProtocolMonitor monitor) {
    this.bufferPool = bufferPool;
    this.requestIDGenerator = requestIDGenerator;
    this.monitor = monitor;
  }

  public int getNextRequestId() {
    return requestIDGenerator.getRequestId();
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
    var messageBuffer = readResponseIntoBuffer(socket, responseHeader);

    int flagBits = messageBuffer.getInt();
    messageBuffer.getByte(); // section byte

    return new Response<>(responseHeader, flagBits, messageBuffer);
  }

  private BobBsonBuffer readResponseIntoBuffer(
      BongoSocket socket, BongoResponseHeader responseHeader) {
    if (responseHeader.getMessageLength() < 0) {
      throw new BongoSocketReadException("Message length < 0 in response header");
    }

    if (responseHeader.getOpCode() == 2012) {
      BobBsonBuffer compressionHeaders = socket.read(9);
      compressionHeaders.getInt(); // original opcode
      int uncompressedSize = compressionHeaders.getInt();
      int compressorId = compressionHeaders.getByte();
      bufferPool.recycle(compressionHeaders);

      var compressor = socket.getCompressor();
      if (compressor == null) {
        throw new BongoException("Decompression required but no compressor is defined in socket");
      } else if (compressorId != compressor.getId()) {
        throw new BongoException(
            "Decompression requested but response contains incompatible compressor ID");
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
      return decompressedMessageBuffer;
    } else {
      return socket.read(responseHeader.getMessageLength() - 16);
    }
  }

  public <T> Response<T> readServerResponse(BongoSocket socket, BobBsonConverter<T> converter) {
    var responseHeader = socket.readResponseHeader();
    var messageBuffer = readResponseIntoBuffer(socket, responseHeader);
    int flagBits = messageBuffer.getInt();
    messageBuffer.getByte(); // section byte

    var reader = new BsonReaderStack(messageBuffer);
    var response = converter.read(reader);
    if (response == null) {
      throw new BongoSocketReadException(
          "Response was null from the server " + socket.getServerAddress());
    }

    if (monitor != null) {
      monitor.onReadServerResponse(messageBuffer);
    }

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

  public <T> int sendCommandMessage(
      BongoSocket socket,
      BobBsonConverter<T> converter,
      @NonNull T value,
      Boolean compress,
      boolean stream,
      @Nullable BongoPayload payload) {
    var requestId = getNextRequestId();
    var buffer =
        prepareCommandMessage(socket, converter, value, compress, stream, payload, requestId);

    for (var buf : buffer.getBuffers()) {
      socket.write(buf);
    }

    if (monitor != null) {
      monitor.onSendCommandMessage(requestId, buffer);
    }

    buffer.release();

    return requestId;
  }

  private <T> void writeContentToBuffer(
      DynamicBobBsonBuffer buffer,
      BobBsonConverter<T> converter,
      T value,
      boolean stream,
      @Nullable BongoPayload payload) {
    var flagBits = 0;
    if (stream) {
      flagBits |= 1 << 16;
    }
    buffer.writeInteger(flagBits); // flag bits
    buffer.writeByte((byte) 0);

    var bsonOutput = new StackBsonWriter(buffer);
    converter.write(bsonOutput, value);

    if (payload != null) {
      buffer.writeByte((byte) 1);
      int position = buffer.getTail();
      buffer.skipTail(4);
      buffer.writeString(payload.getIdentifier());
      buffer.writeByte((byte) 0);

      payload.getItems().write(buffer, payload.getIndexMap());

      buffer.writeInteger(position, buffer.getTail() - position);
    }
  }

  public <T> DynamicBobBsonBuffer prepareCommandMessage(
      BongoSocket socket,
      BobBsonConverter<T> converter,
      @NonNull T value,
      Boolean requestCompress,
      boolean stream,
      @Nullable BongoPayload payload,
      int requestId) {
    var compress = BongoCompressor.shouldCompress(socket.getCompressor(), requestCompress);
    var buffer = new DynamicBobBsonBuffer(bufferPool);
    buffer.skipTail(4);
    buffer.writeInteger(requestId);
    buffer.writeInteger(0);
    buffer.writeInteger(compress ? 2012 : 2013);

    if (compress) {
      var compressor = socket.getCompressor();
      if (compressor == null) {
        throw new BongoException(
            "Compression requested but no compressors are available on socket");
      }
      var contentBuffer = new DynamicBobBsonBuffer(bufferPool);
      writeContentToBuffer(contentBuffer, converter, value, stream, payload);

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
        throw new BongoException("Compressed buffer has inaccessible array");
      }

      buffer.writeBytes(
          compressedBufferArray, compressedBuffer.getHead(), compressedBuffer.getTail());
      bufferPool.recycle(compressedBuffer);

      contentBuffer.release();
    } else {
      writeContentToBuffer(buffer, converter, value, stream, payload);
    }

    buffer.writeInteger(0, buffer.getTail());

    return buffer;
  }

  public record Response<T>(BongoResponseHeader header, int flagBits, T payload) {}
}
