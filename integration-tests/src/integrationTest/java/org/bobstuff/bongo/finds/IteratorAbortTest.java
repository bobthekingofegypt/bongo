package org.bobstuff.bongo.finds;

import de.flapdoodle.embed.mongo.commands.ServerAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MongoDbResolver.class)
public class IteratorAbortTest {
  private BongoClient bongo;
  private BongoSettings settings;
  private Faker faker;

  private List<Company> entries;

  @BeforeEach
  public void setup(@MongoUrl ServerAddress mongoUrl) {
    var bobBson = new BobBson();
    BsonValueConverters.register(bobBson);
    bobBson.registerConverter(ObjectId.class, new BongoObjectIdConverter());

    settings =
        BongoSettings.builder()
            .connectionSettings(
                BongoConnectionSettings.builder()
                    .compressor(new BongoCompressorZstd())
                    .host(mongoUrl.toString())
                    .build())
            .bufferPool(new BobBufferPool())
            .socketPoolProvider(new BongoSocketPoolProviderVibur(1))
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    bongo = new BongoClient(settings);
    bongo.connect();
    faker = new Faker(new Random(23));

    var entries = new ArrayList<Company>();
    for (var i = 0; i < 20; i += 1) {
      entries.add(CompanyDataGenerator.company(faker));
    }

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    collection.insertMany(entries);
  }

  @AfterEach
  public void cleanUp() {
    bongo.close();
  }

  @Test
  public void testSerialStrategyAbortsExhaustibleSocket(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    for (var i = 0; i < 10; i += 1) {
      var strategy = new ReadExecutionSerialStrategy<Company>();
      var iterator =
          collection
              .find(strategy)
              .cursorType(BongoCursorType.Exhaustible)
              .batchSize(2)
              .options(BongoFindOptions.builder().build())
              .iterator();

      iterator.next();
      iterator.next();
      iterator.next();

      iterator.close();
      strategy.close();
    }
  }

  @Test
  public void testConcurrentStrategyAbortsExhaustibleSocket(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    for (var i = 0; i < 10; i += 1) {
      System.out.println("TESTING");
      var strategy = new ReadExecutionConcurrentStrategy<Company>(1, 1);
      var iterator =
          collection
              .find(strategy)
              .cursorType(BongoCursorType.Exhaustible)
              .batchSize(2)
              .options(BongoFindOptions.builder().build())
              .iterator();

      iterator.next();
      iterator.next();
      iterator.next();

      iterator.close();
      strategy.close();
    }
  }
}
