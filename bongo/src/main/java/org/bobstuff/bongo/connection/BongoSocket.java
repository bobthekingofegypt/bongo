package org.bobstuff.bongo.connection;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import javax.net.SocketFactory;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticator;
import org.bobstuff.bongo.compressors.BongoCompressor;
import org.bobstuff.bongo.exception.BongoConnectionException;
import org.bobstuff.bongo.exception.BongoSocketReadException;
import org.bobstuff.bongo.exception.BongoSocketWriteException;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.bobstuff.bongo.messages.BongoResponseHeaderParser;
import org.bobstuff.bongo.topology.ServerAddress;
import org.bobstuff.bongo.topology.ServerDescription;
import org.bson.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class BongoSocket {
  private final ServerAddress serverAddress;
  private final BongoAuthenticator authenticator;
  private final BufferDataPool bufferPool;

  private @Nullable BongoSocketPool socketPool;

  private @MonotonicNonNull Socket socket;

  private @MonotonicNonNull OutputStream outputStream;
  private @MonotonicNonNull InputStream inputStream;

  private final BobBsonBuffer responseHeaderBuffer;

  private boolean closed;
  private @MonotonicNonNull ServerDescription initialServerDescription;

  private WireProtocol wireProtocol;

  private @Nullable BongoCompressor compressor;
  private BongoSocketInitialiser initialiser;

  public BongoSocket(
      ServerAddress serverAddress,
      BongoSocketInitialiser initialiser,
      BongoAuthenticator authenticator,
      WireProtocol wireProtocol,
      BufferDataPool bufferPool) {
    this.serverAddress = serverAddress;
    this.authenticator = authenticator;
    this.initialiser = initialiser;
    this.wireProtocol = wireProtocol;
    this.bufferPool = bufferPool;
    this.responseHeaderBuffer = bufferPool.allocate(16);
    this.closed = false;
  }

  public void setSocketPool(BongoSocketPool socketPool) {
    this.socketPool = socketPool;
  }

  public ServerAddress getServerAddress() {
    return serverAddress;
  }

  public @Nullable BongoCompressor getCompressor() {
    return compressor;
  }

  public boolean isClosed() {
    return closed;
  }

  public void release() {
    if (this.socketPool != null) {
      socketPool.release(this);
    }
  }

  public void open() {
    log.trace("Opening new connection to address {}", serverAddress);
    final SocketFactory socketFactory = SocketFactory.getDefault();
    try {
      socket = socketFactory.createSocket();
      socket.connect(
          new InetSocketAddress(serverAddress.getHost(), serverAddress.getPort()), 30000);
      socket.setTcpNoDelay(true);
      socket.setPerformancePreferences(1, 5, 6);
      outputStream = socket.getOutputStream();
      inputStream = new BufferedInputStream(socket.getInputStream());
    } catch (IOException e) {
      throw new BongoConnectionException(e);
    }

    var initialiserResult = initialiser.initialise(this);
    initialServerDescription = initialiserResult.getServerDescription();
    compressor = initialiserResult.getCompressor();
  }

  public void close() {
    if (socket != null && !closed) {
      log.trace("Closing socket for server {}", serverAddress);
      closed = true;
      try {
        // this will interrupt any threads blocked on accessing this sockets streams
        socket.close();
      } catch (IOException e) {
        // noop
      }
      if (socketPool != null) {
        socketPool.remove(this);
      }
    }
  }

  public ServerDescription getInitialServerDescription() {
    if (initialServerDescription == null) {
      throw new IllegalStateException(
          "getInitialServerDescription called before handshake complete");
    }
    return initialServerDescription;
  }

  public BongoResponseHeader readResponseHeader() {
    if (inputStream == null || outputStream == null) {
      throw new BongoSocketReadException("Socket streams have not been initialised");
    }

    var responseHeaderBufferArray = responseHeaderBuffer.getArray();
    if (responseHeaderBufferArray == null) {
      throw new BongoSocketReadException(
          "Sockets internal header buffer does not have initialised byte array");
    }

    try {
      outputStream.flush();
    } catch (IOException e) {
      throw new BongoSocketReadException("Failed to flush outputstream prior to read", e);
    }
    responseHeaderBuffer.setHead(0);

    int totalBytesRead = 0;
    try {
      while (totalBytesRead < 16) {
        int bytesRead =
            inputStream.read(responseHeaderBufferArray, totalBytesRead, 16 - totalBytesRead);
        if (bytesRead == -1) {
          throw new BongoSocketReadException(
              "Broken connection when reading socket, socket returned read of -1 bytes");
        }
        totalBytesRead += bytesRead;
      }
    } catch (IOException e) {
      throw new BongoSocketReadException("Failed to read header from socket stream", e);
    }
    responseHeaderBuffer.setTail(totalBytesRead);

    return BongoResponseHeaderParser.read(responseHeaderBuffer, new BongoResponseHeader());
  }

  public BobBsonBuffer read(int bytes) {
    if (inputStream == null || outputStream == null) {
      throw new BongoSocketReadException("Socket streams have not been initialised");
    }
    var buffer = bufferPool.allocate(bytes);
    var bufferArray = buffer.getArray();
    if (bufferArray == null) {
      throw new BongoSocketReadException("BobBsonBuffer array cannot be null");
    }

    int totalBytesRead = 0;
    try {
      while (totalBytesRead < bytes) {
        int bytesRead = inputStream.read(bufferArray, totalBytesRead, bytes - totalBytesRead);
        if (bytesRead == -1) {
          throw new RuntimeException("broken reading socket");
        }
        totalBytesRead += bytesRead;
      }
    } catch (IOException e) {
      throw new BongoSocketReadException(
          "Failed to read " + bytes + " bytes from socket stream", e);
    }
    buffer.setTail(totalBytesRead);

    return buffer;
  }

  public void write(@NonNull BobBsonBuffer buf) {
    if (outputStream == null) {
      throw new BongoSocketWriteException("Socket output stream is null");
    }

    var bufferArray = buf.getArray();
    if (bufferArray == null) {
      throw new BongoSocketWriteException("Attempt to write buffer to socket, but buffer is null");
    }

    try {
      outputStream.write(bufferArray, buf.getHead(), buf.getTail());
    } catch (IOException e) {
      throw new BongoSocketWriteException("failed to write to socket", e);
    }
  }

  public void write(@NonNull List<BobBsonBuffer> buffers) {
    for (var buffer : buffers) {
      write(buffer);
    }
  }
}
