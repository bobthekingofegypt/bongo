package org.bobstuff.bongo.bulk;

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
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MongoDbResolver.class)
public class BulkWriteTest {
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

    var operations = new ArrayList<BongoWriteOperation<Company>>();
    operations.add(
        new BongoUpdate<Company>(
            Filters.eq("name", entries.get(0).getName()).toBsonDocument(),
            List.of(Updates.set("name", "nothing happens").toBsonDocument()),
            false));
    operations.add(new BongoInsert<Company>(CompanyDataGenerator.company(faker)));

    collection.bulkWrite(operations);

    var count = collection.count();
    Assertions.assertEquals(4, count);
    var entry = collection.findOne(Filters.eq("name", "nothing happens").toBsonDocument());
    Assertions.assertNotNull(entry);
  }

  @Test
  public void testManyOperationsAggregate(@MongoUrl ServerAddress mongoUrl) {
    var database = bongo.getDatabase("inttest");
    var collection = database.getCollection("companies", Company.class);

    var insertOne =CompanyDataGenerator.company(faker);
    var insertTwo =CompanyDataGenerator.company(faker);
    var insertThree =CompanyDataGenerator.company(faker);
    var insertFour =CompanyDataGenerator.company(faker);

    var operations = new ArrayList<BongoWriteOperation<Company>>();
    operations.add(
            new BongoUpdate<>(
                    Filters.eq("name", entries.get(0).getName()).toBsonDocument(),
                    List.of(Updates.set("name", "nothing happens").toBsonDocument()),
                    false));
    operations.add(new BongoInsert<>(insertOne));
    operations.add(new BongoInsert<>(insertTwo));
    operations.add(new BongoInsert<>(insertThree));
    operations.add(new BongoInsert<>(insertFour));
    operations.add(
            new BongoUpdate<>(
                    Filters.eq("name", entries.get(1).getName()).toBsonDocument(),
                    List.of(Updates.set("name", "some other update").toBsonDocument()),
                    false));

    collection.bulkWrite(operations);

    var count = collection.count();
    Assertions.assertEquals(7, count);
    var entry = collection.findOne(Filters.eq("name", "nothing happens").toBsonDocument());
    Assertions.assertNotNull(entry);
    var someOtherEntry = collection.findOne(Filters.eq("name", "some other update").toBsonDocument());
    Assertions.assertNotNull(someOtherEntry);
    var insertOneEntry = collection.findOne(Filters.eq("name", insertOne.getName()).toBsonDocument());
    Assertions.assertNotNull(insertOneEntry);
    var insertTwoEntry = collection.findOne(Filters.eq("name", insertTwo.getName()).toBsonDocument());
    Assertions.assertNotNull(insertTwoEntry);
    var insertThreeEntry = collection.findOne(Filters.eq("name", insertThree.getName()).toBsonDocument());
    Assertions.assertNotNull(insertThreeEntry);
  }
}
