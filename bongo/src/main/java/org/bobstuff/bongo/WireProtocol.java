package org.bobstuff.bongo;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.DynamicBobBsonBuffer;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoSocketReadException;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.checkerframework.checker.nullness.qual.NonNull;

public class WireProtocol {
  private BufferDataPool bufferPool;

  public WireProtocol(BufferDataPool bufferPool) {
    this.bufferPool = bufferPool;
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

  public <T> Response<T> readServerResponse(BongoSocket socket, BobBsonConverter<T> converter) {
    var responseHeader = socket.readResponseHeader();
    if (responseHeader.getMessageLength() < 0) {
      throw new BongoSocketReadException("Messagelength < 0 in response header");
    }

    var messageBuffer = socket.read(responseHeader.getMessageLength() - 16);
    int flagBits = messageBuffer.getInt();
    byte section = messageBuffer.getByte();
    var reader = new BsonReader(messageBuffer);
    var response = converter.read(reader);
    if (response == null) {
      throw new BongoSocketReadException(
          "Response was null from the server " + socket.getServerAddress());
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
    var requestId = socket.getNextRequestId();
    var buffer = new DynamicBobBsonBuffer(bufferPool);
    buffer.skipTail(4);
    buffer.writeInteger(requestId);
    buffer.writeInteger(0);
    buffer.writeInteger(2013);

    var flagBits = 0;
    if (stream) {
      flagBits |= 1 << 16;
    }
    buffer.writeInteger(flagBits); // flag bits
    buffer.writeByte((byte) 0);

    var bsonOutput = new BsonWriter(buffer);
    converter.write(bsonOutput, value);

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
