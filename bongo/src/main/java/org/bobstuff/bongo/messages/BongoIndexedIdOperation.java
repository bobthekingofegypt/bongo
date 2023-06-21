package org.bobstuff.bongo.messages;

import lombok.Data;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.BsonConverter;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;
import org.bobstuff.bongo.converters.BongoObjectIdConverter;

@Data
@GenerateBobBsonConverter
public class BongoIndexedIdOperation {
  private int index;

  @BsonAttribute("_id")
  @BsonConverter(BongoObjectIdConverter.class)
  private byte[] objectId;
}
