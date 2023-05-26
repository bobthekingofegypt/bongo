package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.pool.ConcurrentBobBsonBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;

public class Insert {
  public static void main(String... args) {
    var bobBson = new BobBson();
    BsonValueConverters.register(bobBson);
    bobBson.registerConverter(ObjectId.class, new BongoObjectIdConverter());

    var settings =
        BongoSettings.builder()
            .connectionSettings(
                BongoConnectionSettings.builder()
                    .compressor(new BongoCompressorZstd())
                    .host("192.168.1.138:27027")
                    //                    .credentials(
                    //                        BongoCredentials.builder()
                    //                            .username("admin")
                    //                            .password("speal")
                    //                            .authSource("admin")
                    //                            .build())
                    .build())
            .bufferPool(new ConcurrentBobBsonBufferPool())
            .socketPoolProvider(new BongoSocketPoolProviderVibur())
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    var bongo = new BongoClient(settings);
    bongo.connect();

    var database = bongo.getDatabase("test_data");
    var collection =
        database
            .getCollection("companies", Company.class)
            .withWriteConcern(new BongoWriteConcern(1, false));

    Faker faker = new Faker(new Random(23));
    var companies = new ArrayList<Company>();
    for (var i = 0; i < 9000; i += 1) {
      companies.add(CompanyDataGenerator.company(faker));
    }
    //    var strategy = new WriteExecutionSerialStrategy<Company>();
    var strategy = new WriteExecutionConcurrentStrategy<Company>(5, 3);
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (var i = 0; i < 1; i += 1) {
      try {
        var result =
            collection.insertMany(
                companies,
                strategy,
                BongoInsertManyOptions.builder().ordered(false).compress(false).build());
      } catch (BongoBulkWriteException e) {
        System.out.println(e.getWriteErrors().size());
      }
    }
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS) / 10);

    strategy.close();
    bongo.close();
  }
}
