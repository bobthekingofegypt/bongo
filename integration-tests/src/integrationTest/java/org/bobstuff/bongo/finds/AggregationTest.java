package org.bobstuff.bongo.finds;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import de.flapdoodle.embed.mongo.commands.ServerAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentFetchers;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.models.CustomResponseClass;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MongoDbResolver.class)
public class AggregationTest {
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

  @Test
  public void testAggregate(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(eq("name", entries.get(0).getName())).toBsonDocument(),
            limit(1).toBsonDocument());
    List<Company> result = null;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      result = collection.aggregate(pipeline, strategy).into(new ArrayList<>());
    }

    Assertions.assertNotNull(result);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(entries.get(0).getName(), result.get(0).getName());
  }

  @Test
  public void testAggregateConcurrentStrategy(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            limit(5).toBsonDocument());
    List<Company> result;
    try (var strategy = new ReadExecutionConcurrentStrategy<Company>(4)) {
      result = collection.aggregate(pipeline, strategy).into(new ArrayList<>());
    }

    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());
  }

  @Test
  public void testAggregateConcurrentFetcherStrategyFails(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            limit(5).toBsonDocument());

    try (var strategy = new ReadExecutionConcurrentFetchers<Company>(4)) {
      Assertions.assertThrows(
          UnsupportedOperationException.class,
          () -> {
            collection.aggregate(pipeline, strategy).into(new ArrayList<>());
          });
    }
  }

  @Test
  public void testAggregateCustomReturnModel(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            group(null, Accumulators.sum("total", "$reviewScore")).toBsonDocument(),
            limit(5).toBsonDocument());

    List<CustomResponseClass> result;
    try (var strategy = new ReadExecutionSerialStrategy<CustomResponseClass>()) {
      result =
          collection
              .aggregate(pipeline, CustomResponseClass.class, strategy)
              .into(new ArrayList<>());
    }

    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(6, result.get(0).getTotal());
  }

  @Test
  public void testAggregateReturnsOutCollection(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            Aggregates.out("outcollection").toBsonDocument());
    List<Company> result;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      result = collection.aggregate(pipeline, strategy).into(new ArrayList<>());
    }

    var aggregationCollection = database.getCollection("outcollection", Company.class);
    List<Company> rawResult;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      rawResult = aggregationCollection.find(strategy).into(new ArrayList<>());
    }
    Assertions.assertEquals(2, rawResult.size());

    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(rawResult, result);
  }

  @Test
  public void testAggregateReturnsOutCollectionDifferentDb(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            Aggregates.out("inttest2", "outcollection").toBsonDocument());
    List<Company> result;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      result = collection.aggregate(pipeline, strategy).into(new ArrayList<>());
    }

    var outDatabase = bongo.getDatabase("inttest2");
    var aggregationCollection = outDatabase.getCollection("outcollection", Company.class);
    List<Company> rawResult;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      rawResult = aggregationCollection.find(strategy).into(new ArrayList<>());
    }
    Assertions.assertEquals(2, rawResult.size());

    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(rawResult, result);
  }

  @Test
  public void testAggregateReturnsMergeCollection(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            Aggregates.merge("outcollection").toBsonDocument());
    List<Company> result;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      result = collection.aggregate(pipeline, strategy).into(new ArrayList<>());
    }

    var aggregationCollection = database.getCollection("outcollection", Company.class);
    List<Company> rawResult;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      rawResult = aggregationCollection.find(strategy).into(new ArrayList<>());
    }
    Assertions.assertEquals(2, rawResult.size());

    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(rawResult, result);
  }

  @Test
  public void testAggregateReturnsMergeCollectionDifferentDb(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            Aggregates.merge(new MongoNamespace("inttest2", "outcollection")).toBsonDocument());
    List<Company> result;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      result = collection.aggregate(pipeline, strategy).into(new ArrayList<>());
    }

    var outDatabase = bongo.getDatabase("inttest2");
    var aggregationCollection = outDatabase.getCollection("outcollection", Company.class);
    List<Company> rawResult;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      rawResult = aggregationCollection.find(strategy).into(new ArrayList<>());
    }
    Assertions.assertEquals(2, rawResult.size());

    Assertions.assertNotNull(result);
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(rawResult, result);
  }

  @Test
  public void testAggregateOutToCollection(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            Aggregates.out("outcollection").toBsonDocument());
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      collection.aggregate(pipeline, strategy).toCollection();
    }

    var aggregationCollection = database.getCollection("outcollection", Company.class);
    List<Company> rawResult;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      rawResult = aggregationCollection.find(strategy).into(new ArrayList<>());
    }
    Assertions.assertEquals(2, rawResult.size());
  }

  @Test
  public void testAggregateMergeToCollection(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline =
        Arrays.asList(
            match(ne("name", entries.get(0).getName())).toBsonDocument(),
            Aggregates.merge("outcollection").toBsonDocument());
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      collection.aggregate(pipeline, strategy).toCollection();
    }

    var aggregationCollection = database.getCollection("outcollection", Company.class);
    List<Company> rawResult;
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      rawResult = aggregationCollection.find(strategy).into(new ArrayList<>());
    }
    Assertions.assertEquals(2, rawResult.size());
  }

  @Test
  public void testAggregateNotMergeOrOutToCollection(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var pipeline = Arrays.asList(match(ne("name", entries.get(0).getName())).toBsonDocument());
    try (var strategy = new ReadExecutionSerialStrategy<Company>()) {
      Assertions.assertThrows(
          BongoException.class, () -> collection.aggregate(pipeline, strategy).toCollection());
    }
  }
}
