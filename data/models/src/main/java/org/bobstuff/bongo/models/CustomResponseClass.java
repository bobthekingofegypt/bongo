package org.bobstuff.bongo.models;

import org.bobstuff.bobbson.annotations.CompiledBson;

@CompiledBson
public class CustomResponseClass {
  private int total;

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }
}
