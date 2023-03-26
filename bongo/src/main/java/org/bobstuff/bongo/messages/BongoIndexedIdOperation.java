package org.bobstuff.bongo.messages;

import lombok.Data;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.BsonConverter;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bobstuff.bongo.converters.BongoObjectIdConverter;

@Data
@CompiledBson
public class BongoIndexedIdOperation {
  private int index;

  @BsonAttribute("_id")
  @BsonConverter(target = BongoObjectIdConverter.class)
  private byte[] objectId;
}