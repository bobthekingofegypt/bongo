package org.bobstuff.bongo.topology;

import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.buffer.pool.BobBsonBufferPool;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.connection.BongoSocketPool;
import org.bobstuff.bongo.exception.BongoConnectionException;
import org.bobstuff.bongo.exception.BongoSdamException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

@Slf4j
class MongoServer {
  private BlockingQueue<ServerDescription> clusterEvents;
  private ServerAddress serverAddress;
  private ServerId serverId;

  private CountDownLatch poolActive = new CountDownLatch(1);

  private BongoSocketPool socketPool;

  private @MonotonicNonNull ServerDescription serverDescription;

  private ExecutorService monitorExecutor;

  private @MonotonicNonNull Future<?> monitorFuture;
  private BobBsonBufferPool bufferPool;
  private BongoCodec codec;

  private @MonotonicNonNull ServerMonitor serverMonitor;

  private WireProtocol wireProtocol;

  public MongoServer(
      ClusterId clusterId,
      ServerAddress serverAddress,
      WireProtocol wireProtocol,
      BongoSocketPool socketPool,
      BobBsonBufferPool bufferPool,
      BongoCodec codec,
      ExecutorService monitorExecutor,
      BlockingQueue<ServerDescription> clusterEvents) {
    this.serverId = new ServerId(clusterId, serverAddress);
    this.serverAddress = serverAddress;
    this.wireProtocol = wireProtocol;
    this.socketPool = socketPool;
    this.bufferPool = bufferPool;
    this.codec = codec;
    this.clusterEvents = clusterEvents;
    this.monitorExecutor = monitorExecutor;
  }

  public boolean isPrimary() {
    return serverDescription != null && serverDescription.isPrimary();
  }

  public ServerAddress getServerAddress() {
    return serverAddress;
  }

  public BongoSocket getConnection() {
    try {
      if (!poolActive.await(30, TimeUnit.SECONDS)) {
        throw new BongoConnectionException("Failed to retrieve connection within timeout limit");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return socketPool.get();
  }

  public void updateServerDescription(ServerDescription serverDescription) {
    poolActive.countDown();

    this.serverDescription = serverDescription;

    try {
      clusterEvents.put(serverDescription);
    } catch (InterruptedException e) {
      throw new BongoSdamException("Failed to submit server event to events queue", e);
    }
  }

  public void startMonitoring() {
    this.serverMonitor =
        new ServerMonitor(
            serverAddress, wireProtocol, socketPool, clusterEvents, bufferPool, codec, this);
    monitorFuture = monitorExecutor.submit(serverMonitor);
  }

  public void stopMonitoring() {
    log.debug("Stopping monitoring for server {}", serverAddress);
    if (serverMonitor != null) {
      serverMonitor.close();
    }
    if (monitorFuture != null) {
      monitorFuture.cancel(true);
    }
  }
}
