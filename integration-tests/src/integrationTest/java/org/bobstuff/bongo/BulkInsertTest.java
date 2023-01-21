package org.bobstuff.bongo;

import de.flapdoodle.embed.mongo.commands.ServerAddress;
import java.util.List;
import java.util.Random;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.exception.BongoBulkWriteException;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionStrategy;
import org.bobstuff.bongo.inserts.WriteStrategyProvider;
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
  }

  @AfterEach
  public void cleanUp() {
    bongo.close();
  }

  @Test
  public void testInsertOne(@MongoUrl ServerAddress mongoUrl) {
    var data = CompanyDataGenerator.company(faker);

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var insertResult = collection.insertOne(data);

    data.setMongoId(new ObjectId(insertResult.getInsertedId()));

    var result =
        collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<>(settings.getCodec().converter(Company.class)))
            .cursor();

    Assertions.assertEquals(data, result.next());
    Assertions.assertNotNull(insertResult);
  }

  @Test
  public void testInsertOneUnacknowledged(@MongoUrl ServerAddress mongoUrl) {
    var data = CompanyDataGenerator.company(faker);

    var database = bongo.getDatabase("inttest");
    var collection =
        database
            .getCollection("companies", Company.class)
            .withWriteConcern(new BongoWriteConcern(0));

    var insertResult = collection.insertOne(data);

    Assertions.assertThrows(UnsupportedOperationException.class, insertResult::getInsertedId);
  }

  @ParameterizedTest
  @ArgumentsSource(WriteStrategyProvider.class)
  public void testBasicInsert(
      WriteExecutionStrategy<Company> input, @MongoUrl ServerAddress mongoUrl) {
    var data = CompanyDataGenerator.company(faker);
    var data2 = CompanyDataGenerator.company(faker);

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var insertResult = collection.insertMany(List.of(data, data2), input);
    input.close();

    data.setMongoId(new ObjectId(insertResult.getInsertedIds().get(0)));
    data2.setMongoId(new ObjectId(insertResult.getInsertedIds().get(1)));

    var result =
        collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<>(settings.getCodec().converter(Company.class)))
            .cursor();

    Assertions.assertEquals(data, result.next());
    Assertions.assertEquals(data2, result.next());
    Assertions.assertNotNull(insertResult);
  }

  @Test
  public void testBasicInsertErrorDuplicateKeyOrdered(@MongoUrl ServerAddress mongoUrl) {
    var id = new ObjectId();
    var data = CompanyDataGenerator.company(faker);
    data.setMongoId(id);
    var data2 = CompanyDataGenerator.company(faker);
    data2.setMongoId(id);
    var data3 = CompanyDataGenerator.company(faker);
    data3.setMongoId(id);

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    BongoBulkWriteException thrown =
        Assertions.assertThrows(
            BongoBulkWriteException.class,
            () -> collection.insertMany(List.of(data, data2, data3)),
            "Expected insertMany() to throw, but it didn't");

    Assertions.assertEquals(1, thrown.getWriteErrors().size());
    var error = thrown.getWriteErrors().get(0);
    Assertions.assertEquals(11000, error.getCode());
    Assertions.assertEquals(1, error.getIndex());
    Assertions.assertNotNull(error.getErrmsg());
  }

  @Test
  public void testBasicInsertErrorDuplicateKeyUnordered(@MongoUrl ServerAddress mongoUrl) {
    var id = new ObjectId();
    var data = CompanyDataGenerator.company(faker);
    data.setMongoId(id);
    var data2 = CompanyDataGenerator.company(faker);
    data2.setMongoId(id);
    var data3 = CompanyDataGenerator.company(faker);
    data3.setMongoId(id);

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    BongoBulkWriteException thrown =
        Assertions.assertThrows(
            BongoBulkWriteException.class,
            () ->
                collection.insertMany(
                    List.of(data, data2, data3),
                    BongoInsertManyOptions.builder().ordered(false).build()),
            "Expected insertMany() to throw, but it didn't");

    Assertions.assertEquals(2, thrown.getWriteErrors().size());

    var error = thrown.getWriteErrors().get(0);
    Assertions.assertEquals(11000, error.getCode());
    Assertions.assertEquals(1, error.getIndex());
    Assertions.assertNotNull(error.getErrmsg());

    var error2 = thrown.getWriteErrors().get(1);
    Assertions.assertEquals(11000, error2.getCode());
    Assertions.assertEquals(2, error2.getIndex());
    Assertions.assertNotNull(error2.getErrmsg());
  }

  @Test
  public void testBasicInsertErrorDuplicateKeyUnorderedConc(@MongoUrl ServerAddress mongoUrl) {
    var id = new ObjectId();
    var data = CompanyDataGenerator.company(faker);
    data.setMongoId(id);
    var data2 = CompanyDataGenerator.company(faker);
    data2.setMongoId(id);
    var data3 = CompanyDataGenerator.company(faker);
    data3.setMongoId(id);

    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    final WriteExecutionStrategy<Company> strategy = new WriteExecutionConcurrentStrategy<>(3, 3);

    BongoBulkWriteException thrown =
        Assertions.assertThrows(
            BongoBulkWriteException.class,
            () ->
                collection.insertMany(
                    List.of(data, data2, data3),
                    strategy,
                    BongoInsertManyOptions.builder().ordered(false).build()),
            "Expected insertMany() to throw, but it didn't");

    strategy.close();

    // TODO check collection size with count to make sure the passing writes actually happened

    Assertions.assertEquals(2, thrown.getWriteErrors().size());

    // no point in checking indexes because on duplicate key who ever is first wins and it could be
    // any thread
    var error = thrown.getWriteErrors().get(0);
    Assertions.assertEquals(11000, error.getCode());
    Assertions.assertNotNull(error.getErrmsg());

    var error2 = thrown.getWriteErrors().get(1);
    Assertions.assertEquals(11000, error2.getCode());
    Assertions.assertNotNull(error2.getErrmsg());
  }
}
