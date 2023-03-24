package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.*;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.models.Person;
import org.bobstuff.bongo.models.Scores;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BulkWriteMongo {
  public static void main(String... args) {
    PojoCodecProvider.Builder providerBuilder = PojoCodecProvider.builder();
    providerBuilder.register(Person.class);
    providerBuilder.register(Scores.class);
    var provider = providerBuilder.build();
    CodecRegistry pojoCodecRegistry =
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
    var codecRegistry =
            CodecRegistries.fromRegistries(
                    MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
    MongoClientSettings settings =
            MongoClientSettings.builder()
                               .codecRegistry(codecRegistry)
                               .applyToSocketSettings(
                                       builder -> {
                                         builder.connectTimeout(1, TimeUnit.DAYS);
                                         builder.readTimeout(1, TimeUnit.DAYS);
                                       })
                               .applyConnectionString(
                                       new ConnectionString(
                                               //
                                               // "mongodb://192.168.1.138:27027,192.168.1.138:27028,192.168.1.138:27029/"))
                                               "mongodb://admin:speal@192.168.1.138:27017/?authSource=admin"))
                               .build();
    var client = MongoClients.create(settings);
    var mongoDatabase = client.getDatabase("test_data");
    var mongoCollection = mongoDatabase.getCollection("companies4", Company.class);

    Faker faker = new Faker(new Random(23));
    var companies = new ArrayList<WriteModel<Company>>();
    for (var i = 0; i < 9000; i += 1) {
      var company = CompanyDataGenerator.company(faker);
      companies.add(new InsertOneModel<>(company));
      if (i % 4 == 0) {
        companies.add(
                new UpdateOneModel<Company>(
                        Filters.eq("_id", new ObjectId()), List.of(Updates.set("nothing", "happening"))));
      }
    }
    //    var strategy = new WriteExecutionSerialStrategy<Company>();
    var strategy = new WriteExecutionSerialStrategy<Company>();
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (var i = 0; i < 1; i += 1) {
      try {
            mongoCollection.bulkWrite(
                companies,
                new BulkWriteOptions().ordered(false)
                );
      } catch (BongoBulkWriteException e) {
        System.out.println(e.getWriteErrors().size());
      }
    }
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS) / 10);

    strategy.close();
    client.close();
  }
}
