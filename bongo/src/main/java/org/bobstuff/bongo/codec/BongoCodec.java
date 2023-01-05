package org.bobstuff.bongo.codec;

import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonReader;

public interface BongoCodec {
  <T> T decode(Class<T> clazz, BsonReader reader);

  <T> BobBsonConverter<T> converter(Class<T> clazz);
}
