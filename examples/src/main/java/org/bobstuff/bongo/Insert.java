package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
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
    var collection = database.getCollection("companies", Company.class);

    Faker faker = new Faker(new Random(23));
    var companies = new ArrayList<Company>();
    for (var i = 0; i < 400000; i += 1) {
      companies.add(CompanyDataGenerator.company(faker));
    }
    //    var people = new ArrayList<Person>();
    //    for (var i = 0; i < 1000000; i += 1) {
    //      var person = new Person();
    //      person.setName(faker.name().fullName());
    //      person.setAge(faker.number().numberBetween(1, 99));
    //      person.setOccupation(faker.job().position());
    //      person.setAddress(faker.lorem().characters(1200));
    //      person.setDescription(faker.lorem().paragraphs(40));
    //      var scores = new Scores();
    //      scores.setScore1(faker.number().randomDouble(2, 0, 1000));
    //      scores.setScore2(faker.number().randomDouble(2, 0, 1000));
    //      scores.setScore3(faker.number().randomDouble(2, 0, 1000));
    //      scores.setScore4(faker.number().randomDouble(2, 0, 1000));
    //      scores.setScore5(faker.number().randomDouble(2, 0, 1000));
    //      scores.setScore6(faker.number().randomDouble(2, 0, 1000));
    //      scores.setScore7(faker.number().randomDouble(2, 0, 1000));
    //      person.setScores(scores);
    //      people.add(person);
    //    }

    var strategy = new WriteExecutionSerialStrategy<Company>();
    //    var strategy = new WriteExecutionConcurrentStrategy<Person>(2, 5);
    Stopwatch stopwatch = Stopwatch.createStarted();
    for (var i = 0; i < 1; i += 1) {
      collection.insertMany(companies, strategy, false);
    }
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS) / 10);

    strategy.close();
    bongo.close();
  }
}
