package org.bobstuff.bongo;

import java.util.List;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@GenerateBobBsonConverter
public class BongoUpdate<TModel> implements BongoWriteOperation<TModel> {
  @BsonAttribute("q")
  private @Nullable BsonDocument filter;

  @BsonAttribute("u")
  private List<BsonDocument> updates;

  @BsonAttribute("multi")
  private boolean multiple;

  private BongoUpdateOptions options;

  public BongoUpdate() {}

  public BongoUpdate(BsonDocument filter, List<BsonDocument> updates, boolean multiple) {
    this(filter, updates, multiple, BongoUpdateOptions.builder().build());
  }

  public BongoUpdate(
      BsonDocument filter,
      List<BsonDocument> updates,
      boolean multiple,
      BongoUpdateOptions options) {
    this.filter = filter;
    this.updates = updates;
    this.multiple = multiple;
    this.options = options;
  }

  public @Nullable BsonDocument getFilter() {
    return filter;
  }

  public void setFilter(BsonDocument filter) {
    this.filter = filter;
  }

  public List<BsonDocument> getUpdates() {
    return updates;
  }

  public void setUpdates(List<BsonDocument> updates) {
    this.updates = updates;
  }

  public boolean isMultiple() {
    return multiple;
  }

  public void setMultiple(boolean multiple) {
    this.multiple = multiple;
  }

  @BsonAttribute("upsert")
  public boolean isUpsert() {
    return options.isUpsert();
  }

  public void setUpsert(boolean upsert) {
    options.setUpsert(upsert);
  }

  @Override
  public BongoWriteOperationType getType() {
    return BongoWriteOperationType.Update;
  }
}
