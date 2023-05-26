package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import com.mongodb.client.model.Aggregates;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.pool.ConcurrentBobBsonBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;

public class Aggregate {
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
            .bufferPool(new ConcurrentBobBsonBufferPool())
            .socketPoolProvider(new BongoSocketPoolProviderVibur())
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    var bongo = new BongoClient(settings);
    bongo.connect();

    var database = bongo.getDatabase("test_data");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            //            Aggregates.match(Filters.eq("owner.occupation",
            // "Manufacturing")).toBsonDocument(),
            Aggregates.limit(1000000).toBsonDocument());

    var iter =
        collection
            .aggregate(pipeline, new ReadExecutionSerialStrategy<>())
            //              .options(BongoFindOptions.builder().limit(1000000).build())
            .compress(false)
            .cursorType(BongoCursorType.Exhaustible)
            .iterator();

    Stopwatch stopwatch = Stopwatch.createStarted();
    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    iter.close();

    System.out.println(i);
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    bongo.close();
  }
}
