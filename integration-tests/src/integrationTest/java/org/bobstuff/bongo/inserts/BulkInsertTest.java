package org.bobstuff.bongo.inserts;

import de.flapdoodle.embed.mongo.commands.ServerAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.pool.ConcurrentBobBsonBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@ExtendWith(MongoDbResolver.class)
public class BulkInsertTest {
  private BongoClient bongo;
  private BongoSettings settings;
  private Faker faker;

  private BongoDatabase database;
  private BongoCollection<Company> collection;

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
                    .compressor(new BongoCompressorZstd(3))
                    .host(mongoUrl.toString())
                    .build())
            .bufferPool(new ConcurrentBobBsonBufferPool())
            .socketPoolProvider(new BongoSocketPoolProviderVibur())
            .codec(new BongoCodecBobBson(bobBson))
            .build();

    bongo = new BongoClient(settings);
    bongo.connect();
    faker = new Faker(new Random(23));

    database = bongo.getDatabase("inttest");
    collection = database.getCollection("companies", Company.class);

    entries = new ArrayList<>();
    for (var i = 0; i < 4000; i += 1) {
      entries.add(CompanyDataGenerator.company(faker));
    }
  }

  @AfterEach
  public void cleanUp() {
    bongo.close();
  }

  @Test
  public void testInsertOne() {
    var data = entries.get(0);

    var insertResult = collection.insertOne(data);
    Assertions.assertNotNull(insertResult);

    data.setMongoId(new ObjectId(insertResult.getInsertedId()));

    var result = collection.findOne();

    Assertions.assertEquals(data, result);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(org.bobstuff.bongo.WriteStrategyProvider.class)
  public void testInsertOneStrategy(
      WriteStrategyProvider.WriteStrategyWrapper<Company> strategyWrapper) {
    var data = CompanyDataGenerator.company(faker);
    var strategy = strategyWrapper.getStrategy();
    if (strategy instanceof WriteExecutionConcurrentStrategy<Company> s) {
      if (s.getWriters() > 1 || s.getSenders() > 1) {
        Assertions.assertThrows(
            BongoException.class, () -> collection.insertOne(data, strategyWrapper.getStrategy()));
        return;
      }
    }

    var insertResult = collection.insertOne(data, strategyWrapper.getStrategy());
    strategyWrapper.getStrategy().close();

    Assertions.assertNotNull(insertResult);

    data.setMongoId(new ObjectId(insertResult.getInsertedId()));

    var result = collection.findOne();

    Assertions.assertEquals(data, result);
  }

  @Test
  public void testInsertOneUnacknowledged() {
    var collection =
        database
            .getCollection("companies", Company.class)
            .withWriteConcern(new BongoWriteConcern(0));

    var insertResult = collection.insertOne(entries.get(0));

    Assertions.assertThrows(UnsupportedOperationException.class, insertResult::getInsertedId);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(org.bobstuff.bongo.WriteStrategyProvider.class)
  public void testInsertMany(WriteStrategyProvider.WriteStrategyWrapper<Company> strategyWrapper) {
    var insertResult =
        collection.insertMany(entries, strategyWrapper.getStrategy(), strategyWrapper.getOptions());
    strategyWrapper.getStrategy().close();

    Assertions.assertEquals(entries.size(), insertResult.getInsertedIds().size());

    var i = 0;
    for (var entry : entries) {
      entry.setMongoId(new ObjectId(insertResult.getInsertedIds().get(i)));
      i += 1;
    }

    try (var strategy = new ReadExecutionSerialStrategy<Company>();
        var result = collection.find(strategy).iterator()) {
      while (result.hasNext()) {
        var nextCheck = result.next();
        var found = false;
        for (var entry : entries) {
          if (entry.getMongoId().equals(nextCheck.getMongoId())) {
            found = true;
            Assertions.assertEquals(entry, nextCheck);
            break;
          }
        }
        Assertions.assertTrue(found, "did not find a match to insert in db");
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderAll.class)
  public void testInsertManyUnacknowledged(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var collection =
        database
            .getCollection("companies", Company.class)
            .withWriteConcern(new BongoWriteConcern(0));
    var insertResult =
        collection.insertMany(
            entries, strategyWrapper.getStrategy(), strategyWrapper.getInsertManyOptions());
    strategyWrapper.getStrategy().close();

    Assertions.assertThrows(UnsupportedOperationException.class, insertResult::getInsertedIds);
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testBasicInsertErrorDuplicateKeyOrdered(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var id = new ObjectId();
    for (var entry : entries) {
      entry.setMongoId(id);
    }

    BongoBulkWriteException thrown =
        Assertions.assertThrows(
            BongoBulkWriteException.class,
            () -> collection.insertMany(entries),
            "Expected insertMany() to throw, but it didn't");

    Assertions.assertEquals(1, thrown.getWriteErrors().size());
    var error = thrown.getWriteErrors().get(0);
    Assertions.assertEquals(11000, error.getCode());
    Assertions.assertEquals(1, error.getIndex());
    Assertions.assertNotNull(error.getErrmsg());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderUnordered.class)
  public void testBasicInsertErrorDuplicateKeyUnordered(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var id = new ObjectId();
    for (var entry : entries) {
      entry.setMongoId(id);
    }

    BongoBulkWriteException thrown =
        Assertions.assertThrows(
            BongoBulkWriteException.class,
            () ->
                collection.insertMany(
                    entries, strategyWrapper.getStrategy(), strategyWrapper.getInsertManyOptions()),
            "Expected insertMany() to throw, but it didn't");

    strategyWrapper.getStrategy().close();
    Assertions.assertEquals(entries.size() - 1, thrown.getWriteErrors().size());

    var error = thrown.getWriteErrors().get(0);
    Assertions.assertEquals(11000, error.getCode());
    Assertions.assertNotNull(error.getErrmsg());

    var error2 = thrown.getWriteErrors().get(1);
    Assertions.assertEquals(11000, error2.getCode());
    Assertions.assertNotNull(error2.getErrmsg());
  }
}
