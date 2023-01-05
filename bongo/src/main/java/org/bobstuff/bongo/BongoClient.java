package org.bobstuff.bongo;

import org.bobstuff.bongo.exception.BongoConnectionException;
import org.bobstuff.bongo.topology.TopologyManager;
import org.checkerframework.checker.nullness.qual.NonNull;

public class BongoClient {
  private BongoSettings settings;
  private TopologyManager topology;

  public BongoClient(@NonNull BongoSettings settings) {
    // TODO verify all settings are actually valid for what we need to proceed, throw if not
    this.settings = settings;
    this.topology = new TopologyManager(settings);
  }

  /**
   * sync method to initiate connection to mongo database. This will trigger service discovery and
   * monitoring process ensuring there at least a valid readable socket connection available for
   * use.
   *
   * @throws BongoConnectionException if connection fails for any reason
   */
  public void connect() throws BongoConnectionException {
    this.topology.initialise();
  }

  public void close() {
    this.topology.close();
  }

  public void test() {
    this.topology.getReadConnection();
  }
}
