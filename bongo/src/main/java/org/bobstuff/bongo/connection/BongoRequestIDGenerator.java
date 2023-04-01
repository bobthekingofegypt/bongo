package org.bobstuff.bongo.connection;

import java.util.concurrent.atomic.AtomicInteger;

public class BongoRequestIDGenerator {
  private final AtomicInteger requestId = new AtomicInteger(0);

  public int getRequestId() {
    return requestId.getAndIncrement();
  }
}
