package org.bobstuff.bongo.topology;

import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.*;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.connection.BongoSocketPool;
import org.bobstuff.bongo.exception.BongoSdamException;
import org.bobstuff.bongo.exception.BongoSocketReadException;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class ServerMonitor implements Runnable {
  private WireProtocol wireProtocol;
  private BongoSocketPool socketPool;
  private @Nullable BongoSocket socket;

  private BlockingQueue<ServerDescription> clusterEvents;

  private ServerDescription currentServerDescription;
  private boolean closed = false; // TODO should belong to ClusterServer
  private BufferDataPool bufferPool;
  private BongoCodec codec;
  private BobBsonConverter<HelloRequest> helloRequestCodec;

  private boolean startedStreaming;

  private MongoServer clusterServer;

  private boolean isClosed = false;
  private ServerAddress serverAddress;

  public ServerMonitor(
      ServerAddress serverAddress,
      WireProtocol wireProtocol,
      BongoSocketPool socketPool,
      BlockingQueue<ServerDescription> clusterEvents,
      BufferDataPool bufferPool,
      BongoCodec codec,
      MongoServer listener) {
    this.serverAddress = serverAddress;
    this.socketPool = socketPool;
    this.wireProtocol = wireProtocol;
    currentServerDescription =
        ServerDescription.builder().serverType(ServerType.Unknown).address(serverAddress).build();
    this.clusterEvents = clusterEvents;
    this.bufferPool = bufferPool;
    this.codec = codec;
    this.helloRequestCodec = codec.converter(HelloRequest.class);
    this.clusterServer = listener;
  }

  @Override
  public void run() {
    while (!isClosed && !Thread.interrupted()) {
      try {
        var previousServerDescription = currentServerDescription;
        var nextServerDescription = lookupServerDescription(currentServerDescription);

        if (previousServerDescription == null
            || !previousServerDescription.equals(nextServerDescription)) {
          log.trace(
              "Server monitor ({}) state updated. New state: {}",
              serverAddress,
              nextServerDescription);
          this.currentServerDescription = nextServerDescription;
          this.clusterServer.updateServerDescription(nextServerDescription);
        }
      } catch (Throwable e) {
        if (socket != null) {
          try {
            socket.close();
          } catch (Throwable t) {
            // noop
          }
          socket = null;
          startedStreaming = false;
        }
        this.clusterServer.updateServerDescription(
            ServerDescription.builder()
                .serverType(ServerType.Unknown)
                .address(serverAddress)
                .build());

        // TODO wait for next window, interruptable if required
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
          // no-op
        }
      }
    }
  }

  private ServerDescription lookupServerDescription(
      final ServerDescription currentServerDescription) {
    if (socket == null) {
      log.trace("Initialising new socket connection for monitoring server {}", serverAddress);
      socket = socketPool.getNonPooled();
      return socket.getInitialServerDescription();
    }

    log.debug("looking up server description for socket address {}", socket.getServerAddress());
    var socket = this.socket;
    if (socket == null) {
      throw new BongoSdamException("Socket in invalid state during server monitoring");
    }

    if (!startedStreaming) {
      var request = HelloRequest.create(currentServerDescription);
      wireProtocol.sendCommandMessage(socket, helloRequestCodec, request, false, true);
      startedStreaming = true;
    }

    if (Thread.interrupted()) {
      throw new BongoSocketReadException(
          "Thread interrupted reading from socket for " + serverAddress);
    }

    var helloResponse =
        wireProtocol.readServerResponse(socket, codec.converter(HelloResponse.class)).payload();
    var description = ServerDescription.from(socket.getServerAddress(), helloResponse);
    log.debug("Received server description: {} from {}", description, serverAddress);

    return description;
  }

  public void close() {
    isClosed = true;
    if (socket != null) {
      socket.close();
    }
  }
}
