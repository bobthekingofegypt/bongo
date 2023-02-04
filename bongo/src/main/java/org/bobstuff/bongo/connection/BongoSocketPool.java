package org.bobstuff.bongo.connection;

import org.checkerframework.checker.nullness.qual.NonNull;

public interface BongoSocketPool extends BongoSocketPoolMBean {
  @NonNull
  BongoSocket getNonPooled();

  @NonNull
  BongoSocket get();

  void release(@NonNull BongoSocket socket);

  void remove(@NonNull BongoSocket socket);
}
