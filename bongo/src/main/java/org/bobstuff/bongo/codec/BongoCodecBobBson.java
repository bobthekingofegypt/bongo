package org.bobstuff.bongo.codec;

import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bongo.BongoWriteConcern;
import org.bobstuff.bongo.exception.BongoCodecException;
import org.bobstuff.bongo.messages.BongoWriteConcernConverter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BongoCodecBobBson implements BongoCodec {
  private BobBson bobBson;

  public BongoCodecBobBson(BobBson bobBson) {
    this.bobBson = bobBson;
    bobBson.registerConverter(BongoWriteConcern.class, new BongoWriteConcernConverter());
  }

  @Override
  public <T> @Nullable T decode(Class<T> clazz, BsonReader reader) {
    return converter(clazz).read(reader);
  }

  @Override
  public <T> BobBsonConverter<T> converter(Class<T> clazz) {
    var converter = (BobBsonConverter<T>) bobBson.tryFindConverter(clazz);
    if (converter == null) {
      throw new BongoCodecException("No codec found for type " + clazz.getName());
    }

    return converter;
  }
}
