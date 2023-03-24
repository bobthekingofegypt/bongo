package org.bobstuff.bongo.strategies;

import de.flapdoodle.embed.mongo.commands.ServerAddress;
import java.util.List;
import java.util.Random;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MongoDbResolver.class)
public class WriteExecutionCloseTest {
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
            .socketPoolProvider(new BongoSocketPoolProviderVibur())
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    bongo = new BongoClient(settings);
    bongo.connect();
    faker = new Faker(new Random(23));

    var data = CompanyDataGenerator.company(faker);
    var data2 = CompanyDataGenerator.company(faker);
    var data3 = CompanyDataGenerator.company(faker);

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    entries = List.of(data, data2, data3);

    collection.insertMany(entries);
  }

  @AfterEach
  public void cleanUp() {
    bongo.close();
  }

  //  @Test
  //  public void testAutocloseStrategy(@MongoUrl ServerAddress mongoUrl) {
  //    var database = bongo.getDatabase("inttest");
  //    var collection = database.getCollection("companies", Company.class);
  //    var strategy = new WriteExecutionConcurrentStrategy<Company>(1, 1);
  //    try (strategy) {
  //      collection.insertMany(List.of(CompanyDataGenerator.company(faker)), strategy);
  //    }
  //
  //    Assertions.assertTrue(strategy.isClosed());
  //  }
  //
  //  @Test
  //  public void testClosedStrategyThrowsException(@MongoUrl ServerAddress mongoUrl) {
  //    var database = bongo.getDatabase("inttest");
  //    var collection = database.getCollection("companies", Company.class);
  //    var strategy = new WriteExecutionConcurrentStrategy<Company>(1, 1);
  //    try (strategy) {
  //      collection.insertMany(List.of(CompanyDataGenerator.company(faker)), strategy);
  //    }
  //
  //    Assertions.assertThrows(
  //        BongoException.class,
  //        () -> collection.insertMany(List.of(CompanyDataGenerator.company(faker)), strategy));
  //  }

  @Test
  public void testClosedSerialStrategyDoesntThrowsException(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);
    var strategy = new WriteExecutionSerialStrategy<Company>();
    try (strategy) {
      collection.insertMany(List.of(CompanyDataGenerator.company(faker)), strategy);
    }

    collection.insertMany(List.of(CompanyDataGenerator.company(faker)), strategy);
  }
}
