package org.bobstuff.bongo;

import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@CompiledBson
public class BongoDelete<TModel> implements BongoWriteOperation<TModel> {
  @BsonAttribute("q")
  private @Nullable BsonDocument filter;

  private int limit;

  private BongoDeleteOptions options;

  public BongoDelete() {}

  public BongoDelete(BsonDocument filter, boolean multiple) {
    this(filter, multiple, BongoDeleteOptions.builder().build());
  }

  public BongoDelete(BsonDocument filter, boolean multiple, BongoDeleteOptions options) {
    this.filter = filter;
    this.limit = multiple ? 0 : 1;
    this.options = options;
  }

  public @Nullable BsonDocument getFilter() {
    return filter;
  }

  public void setFilter(BsonDocument filter) {
    this.filter = filter;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  //  public boolean isMultiple() {
  //    return multiple;
  //  }
  //
  //  public void setMultiple(boolean multiple) {
  //    this.multiple = multiple;
  //  }
  //
  @Override
  public BongoWriteOperationType getType() {
    return BongoWriteOperationType.Delete;
  }
}
