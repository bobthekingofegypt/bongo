package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentFetchers;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;

public class MultiFind {
  public static void main(String... args) {
    var bobBson = new BobBson();
    BsonValueConverters.register(bobBson);
    bobBson.registerConverter(ObjectId.class, new BongoObjectIdConverter());

    var settings =
        BongoSettings.builder()
            .connectionSettings(
                BongoConnectionSettings.builder()
                    .compressor(new BongoCompressorZstd())
                    .host("192.168.1.138:27017")
                    .credentials(
                        BongoCredentials.builder()
                            .username("admin")
                            .password("speal")
                            .authSource("admin")
                            .build())
                    .build())
            .bufferPool(new BobBufferPool())
            .socketPoolProvider(new BongoSocketPoolProviderVibur())
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    var bongo = new BongoClient(settings);
    bongo.connect();

    var database = bongo.getDatabase("test_data");
    var collection = database.getCollection("companies", Company.class);

    for (var x = 0; x < 1; x += 1) {
      System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++");
      var strategy =
          new ReadExecutionConcurrentFetchers<Company>(
              3, () -> new ReadExecutionConcurrentStrategy<>(3));
      //      var strategy = new ReadExecutionConcurrentFetchers<Company>(3);
      Stopwatch stopwatch = Stopwatch.createStarted();
      var iter =
          collection
              .find(strategy)
              .options(BongoFindOptions.builder().build())
              .compress(false)
              .cursorType(BongoCursorType.Exhaustible)
              .iterator();

      int i = 0;
      while (iter.hasNext()) {
        iter.next();
        i += 1;
      }

      iter.close();
      strategy.close();

      System.out.println(i);
      System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    bongo.close();
  }
}
