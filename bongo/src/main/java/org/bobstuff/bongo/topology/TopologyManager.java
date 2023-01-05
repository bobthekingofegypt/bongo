package org.bobstuff.bongo.topology;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bongo.BongoSettings;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.auth.BongoAuthenticators;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoConnectionException;

@Slf4j
public class TopologyManager implements BongoConnectionProvider {
  private static final ServerDescription POISON_DESCRIPTION =
      ServerDescription.builder().serverType(ServerType.RSOther).build();

  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private ExecutorService topologyExecutor = Executors.newCachedThreadPool();
  private ConcurrentHashMap<ServerAddress, MongoServer> servers;
  private Lock lock = new ReentrantLock();

  private BlockingQueue<ServerDescription> clusterEvents;

  private BongoSettings settings;

  private ClusterId clusterId;

  private CountDownLatch initialisationLatch = new CountDownLatch(1);

  private WireProtocol wireProtocol;

  public TopologyManager(BongoSettings settings, WireProtocol wireProtocol) {
    this.clusterId = new ClusterId();
    this.servers = new ConcurrentHashMap<>();
    this.clusterEvents = new ArrayBlockingQueue<>(10);
    this.settings = settings;
    this.wireProtocol = wireProtocol;
  }

  public void initialise() {
    log.trace("Initialising the topology manager");
    topologyExecutor.execute(new ServerDescriptionEventProcessor());
    try {
      lock.lock();

      log.trace("Adding initial host list: {}", settings.getConnectionSettings().getHosts());
      for (var host : settings.getConnectionSettings().getHosts()) {
        addServer(new ServerAddress(host));
      }
    } finally {
      initialisationLatch.countDown();
      lock.unlock();
    }
  }

  public BongoSocket getReadConnection() {
    var startTime = System.nanoTime();
    while (true) {
      for (var clusterServer : servers.values()) {
        log.trace(
            "Checking server {} is usable for read request", clusterServer.getServerAddress());
        if (clusterServer.isPrimary()) {
          log.trace("Server {} is a primary", clusterServer.getServerAddress());
          return clusterServer.getConnection();
        }
      }

      try {
        log.trace("Attempting to await initialisation latch");
        if (!initialisationLatch.await(10, TimeUnit.SECONDS)) {
          log.debug("Failed to receive initialisation latch, trying again");
          continue;
        }
        log.debug("Passed initialisation latch");
      } catch (InterruptedException e) {
        throw new BongoConnectionException(
            "Thread interrupted awaiting topology initialisation", e);
      }

      var currentTime = System.nanoTime() - startTime;
      if (TimeUnit.SECONDS.convert(currentTime, TimeUnit.NANOSECONDS) > 25) {
        break;
      }
      log.debug("no primaries so lets reset the latch and wait for another update");
    }
    throw new RuntimeException("no connections available");
  }

  public BongoSocket getReadConnection(ServerAddress serverAddress) {
    var server = servers.get(serverAddress);
    if (server == null) {
      throw new BongoConnectionException("No connections available to server " + serverAddress);
    }
    return server.getConnection();
  }

  public void close() {
    for (MongoServer server : servers.values()) {
      server.stopMonitoring();
    }
    clusterEvents.add(POISON_DESCRIPTION);

    executor.shutdown();
    topologyExecutor.shutdown();
  }

  public void addServer(ServerAddress serverAddress) {
    if (servers.containsKey(serverAddress)) {
      log.trace("Attempt to add server {} which already exists", serverAddress);
      return;
    }

    log.debug("Adding new server {} to topology", serverAddress);
    var socketPool =
        settings
            .getSocketPoolProvider()
            .provide(
                serverAddress,
                BongoAuthenticators.from(settings),
                wireProtocol,
                settings.getBufferPool());

    var mongoServer =
        new MongoServer(
            clusterId,
            serverAddress,
            wireProtocol,
            socketPool,
            settings.getBufferPool(),
            settings.getCodec(),
            topologyExecutor,
            clusterEvents);
    servers.put(serverAddress, mongoServer);

    mongoServer.startMonitoring();
  }

  public void update(ServerDescription latestServerDescription) {
    log.debug("received update description {}", latestServerDescription);
    log.debug("update looking for address {}", latestServerDescription.getAddress());
    log.debug("server size {}", servers.size());

    var server = servers.get(latestServerDescription.getAddress());
    if (server == null) {
      log.debug("Server has been removed so ignore the event");
      // server has been removed so ignore the event
      return;
    }

    // handle cluster state like name

    if (latestServerDescription.getHosts() != null) {
      for (var host : latestServerDescription.getHosts()) {
        log.debug("adding new server for address {}", host);
        addServer(host);
      }
    }
    log.debug("+++++++++++++++++++++++++++++++++++++++++++");
    initialisationLatch.countDown();
    initialisationLatch = new CountDownLatch(1);
  }

  public class ServerDescriptionEventProcessor implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          var event = clusterEvents.take();
          if (Thread.interrupted() || event == POISON_DESCRIPTION) {
            break;
          }
          try {
            lock.lock();
            update(event);
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
