package org.bobstuff.bongo;

import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.NoopBufferDataPool;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public class Main {
  public static void main(String... args) {
    var bobBson = new BobBson();
    BsonValueConverters.register(bobBson);
    bobBson.registerConverter(ObjectId.class, new BongoObjectIdConverter());

    var settings =
        BongoSettings.builder()
            .connectionSettings(
                BongoConnectionSettings.builder()
                    .host("192.168.1.138:27027")
                    //                    .credentials(
                    //                        BongoCredentials.builder()
                    //                            .username("admin")
                    //                            .password("speal")
                    //                            .authSource("admin")
                    //                            .build())
                    .build())
            .bufferPool(
                new NoopBufferDataPool((size) -> new BobBufferBobBsonBuffer(new byte[size], 0, 0)))
            .socketPoolProvider(new BongoSocketPoolProviderVibur())
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    var bongo = new BongoClient(settings);
    bongo.connect();

    var database = bongo.getDatabase("test_data");
    var collection = database.getCollection("people", BsonDocument.class);

    var iter =
        collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<BsonDocument>(
                    settings.getCodec().converter(BsonDocument.class)))
            .cursor();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    System.out.println(i);
    //    try {
    //      Thread.sleep(50000);
    //    } catch (InterruptedException e) {
    //      throw new RuntimeException(e);
    //    }
    //
        bongo.close();
    //
    //    try {
    //      Thread.sleep(10000);
    //    } catch (InterruptedException e) {
    //      throw new RuntimeException(e);
    //    }
  }
}
