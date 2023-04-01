package org.bobstuff.bongo.updates;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
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
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@ExtendWith(MongoDbResolver.class)
public class UpdateOneTest {
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
    data.getOwner().setName("Bob");
    var data2 = CompanyDataGenerator.company(faker);
    data2.getOwner().setName("Bob");
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

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testUpdateOne(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var result =
        collection.updateOne(
            Filters.eq("name", entries.get(0).getName()).toBsonDocument(),
            List.of(Updates.set("description", "something something something").toBsonDocument()),
            BongoUpdateOptions.builder().build(),
            strategyWrapper.getStrategy());

    Assertions.assertEquals(1, result.getMatchedCount());
    Assertions.assertEquals(1, result.getModifiedCount());

    var company = collection.findOne(Filters.eq("name", entries.get(0).getName()).toBsonDocument());

    Assertions.assertEquals("something something something", company.getDescription());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testUpdateOneMultipleConditions(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    collection.updateOne(
        Filters.eq("name", entries.get(0).getName()).toBsonDocument(),
        List.of(
            Updates.set("description", "something something something").toBsonDocument(),
            Updates.set("reviewScore", 14).toBsonDocument()),
        BongoUpdateOptions.builder().build(),
        strategyWrapper.getStrategy());

    var company = collection.findOne(Filters.eq("name", entries.get(0).getName()).toBsonDocument());

    Assertions.assertEquals("something something something", company.getDescription());
    Assertions.assertEquals(14, company.getReviewScore());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testUpdateOneOnlyUpdatedOne(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    collection.updateOne(
        Filters.eq("name", entries.get(0).getName()).toBsonDocument(),
        List.of(
            Updates.set("description", "something something something").toBsonDocument(),
            Updates.set("reviewScore", 14).toBsonDocument()),
        BongoUpdateOptions.builder().build(),
        strategyWrapper.getStrategy());

    var company = collection.findOne(Filters.eq("name", entries.get(0).getName()).toBsonDocument());
    var otherCompany =
        collection.findOne(Filters.ne("name", entries.get(0).getName()).toBsonDocument());

    Assertions.assertEquals("something something something", company.getDescription());
    Assertions.assertEquals(14, company.getReviewScore());
    Assertions.assertNotEquals("something something something", otherCompany.getDescription());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testUpdateMany(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    collection.updateMany(
        Filters.eq("owner.name", "Bob").toBsonDocument(),
        List.of(
            Updates.set("description", "something something something").toBsonDocument(),
            Updates.set("reviewScore", 14).toBsonDocument()),
        BongoUpdateOptions.builder().build(),
        strategyWrapper.getStrategy());

    var companies =
        collection
            .find(new ReadExecutionSerialStrategy<>())
            .filter(Filters.eq("owner.name", "Bob").toBsonDocument())
            .into(new ArrayList<>());
    var otherCompany = collection.findOne(Filters.ne("owner.name", "Bob").toBsonDocument());

    Assertions.assertEquals(2, companies.size());
    Assertions.assertEquals("something something something", companies.get(0).getDescription());
    Assertions.assertEquals("something something something", companies.get(1).getDescription());
    Assertions.assertEquals(14, companies.get(0).getReviewScore());
    Assertions.assertEquals(14, companies.get(1).getReviewScore());

    Assertions.assertNotEquals("something something something", otherCompany.getDescription());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testUpdateOneWithUpsert(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    collection.updateOne(
        Filters.eq("owner.name", "Bobbyboy").toBsonDocument(),
        List.of(
            Updates.set("description", "something something something").toBsonDocument(),
            Updates.set("reviewScore", 14).toBsonDocument()),
        BongoUpdateOptions.builder().upsert(true).build(),
        strategyWrapper.getStrategy());

    var companies = collection.find(new ReadExecutionSerialStrategy<>()).into(new ArrayList<>());
    var otherCompany = collection.findOne(Filters.ne("owner.name", "Bobbyboy").toBsonDocument());

    Assertions.assertEquals(4, companies.size());
    Assertions.assertNotEquals("something something something", otherCompany.getDescription());
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(WriteExecutionStrategyProvider.WriteExecutionStrategyProviderOrdered.class)
  public void testUpdateManyWithUpsert(
      WriteExecutionStrategyProvider.WriteExecutionStrategyWrapper<Company> strategyWrapper) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    collection.updateMany(
        Filters.eq("owner.name", "Bobbyboy").toBsonDocument(),
        List.of(
            Updates.set("description", "something something something").toBsonDocument(),
            Updates.set("reviewScore", 14).toBsonDocument()),
        BongoUpdateOptions.builder().upsert(true).build(),
        strategyWrapper.getStrategy());

    var companies = collection.find(new ReadExecutionSerialStrategy<>()).into(new ArrayList<>());
    var otherCompany = collection.findOne(Filters.ne("owner.name", "Bobbyboy").toBsonDocument());

    Assertions.assertEquals(4, companies.size());
    Assertions.assertNotEquals("something something something", otherCompany.getDescription());
  }
}
