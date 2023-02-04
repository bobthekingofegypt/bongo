package org.bobstuff.bongo.vibur;

import java.util.concurrent.atomic.AtomicInteger;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.connection.BongoSocketPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.ConcurrentLinkedQueueCollection;

// TODO separate this class out to its own module
public class BongoSocketPoolVibur implements BongoSocketPool {
  private PoolService<BongoSocket> pool;

  private BongoSocketFactoryVibur socketFactory;

  private AtomicInteger leasedCount = new AtomicInteger();

  public BongoSocketPoolVibur(BongoSocketFactoryVibur socketFactory, int initialSize, int maxSize) {
    this.socketFactory = socketFactory;
    pool =
        new ConcurrentPool<>(
            new ConcurrentLinkedQueueCollection<>(), socketFactory, initialSize, maxSize, true);
  }

  @Override
  public @NonNull BongoSocket getNonPooled() {
    return socketFactory.create();
  }

  @Override
  public BongoSocket get() {
    leasedCount.getAndIncrement();

    var socket = pool.take();
    socket.setSocketPool(this);
    return socket;
  }

  @Override
  public void release(BongoSocket socket) {
    leasedCount.getAndDecrement();
    pool.restore(socket);
  }

  @Override
  public void remove(@NonNull BongoSocket socket) {
    leasedCount.getAndDecrement();
    pool.restore(socket, false);
  }

  @Override
  public int getLeasedCount() {
    return leasedCount.get();
  }
}
