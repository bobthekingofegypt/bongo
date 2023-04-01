package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;

public class BulkWrite {
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
    var collection =
        database
            .getCollection("companiestemp", Company.class)
            .withWriteConcern(new BongoWriteConcern(1, false));

    Faker faker = new Faker(new Random(23));
    var companies = new ArrayList<BongoWriteOperation<Company>>();
    for (var i = 0; i < 90000; i += 1) {
      var company = CompanyDataGenerator.company(faker);
      companies.add(new BongoInsert<>(company));
      if (i % 4 == 0) {
        companies.add(
            new BongoUpdate<>(
                Filters.eq("_id", new ObjectId()).toBsonDocument(),
                List.of(Updates.set("nothing", "happening").toBsonDocument()),
                false));
      }
    }
    //    var strategy = new WriteExecutionSerialStrategy<Company>();
    var strategy = new WriteExecutionSerialStrategy<Company>();
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (var i = 0; i < 1; i += 1) {
      try {
        collection.bulkWrite(
            companies,
            BongoBulkWriteOptions.builder().ordered(true).compress(false).build(),
            strategy);
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
