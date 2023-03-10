package org.bobstuff.bongo;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.BsonDocument;
import org.checkerframework.checker.nullness.qual.Nullable;

@CompiledBson
public class BongoUpdate<TModel> implements BongoWriteOperation<TModel> {
  @BsonAttribute("q")
  private @Nullable BsonDocument filter;

  @BsonAttribute("u")
  private List<BsonDocument> updates;

  @BsonAttribute("multi")
  private boolean multiple;

  public BongoUpdate() {

  }

  public BongoUpdate(BsonDocument filter, List<BsonDocument> updates, boolean multiple) {
    this.filter = filter;
    this.updates = updates;
    this.multiple = multiple;
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

  @Override
  public BongoWriteOperationType getType() {
    return BongoWriteOperationType.Update;
  }
}
