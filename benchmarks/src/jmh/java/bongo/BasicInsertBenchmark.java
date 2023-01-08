package bongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.models.Person;
import org.bobstuff.bongo.models.Scores;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 10000, timeUnit = MILLISECONDS)
public class BasicInsertBenchmark {
  @State(Scope.Benchmark)
  public static class MyBongoClient {
    public BongoClient bongoClient;
    public BongoDatabase bongoDatabase;
    public BongoCollection<Person> collection;

    public BongoCodec codec;

    public ReadExecutionConcurrentStrategy<Person> concurrentStrategy;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      var bobBson = new BobBson();
      BsonValueConverters.register(bobBson);
      bobBson.registerConverter(ObjectId.class, new BongoObjectIdConverter());

      var settings =
          BongoSettings.builder()
              .connectionSettings(
                  BongoConnectionSettings.builder()
                      .host("192.168.1.138:27027")
                      .compressor(new BongoCompressorZstd())
                      .build())
              .bufferPool(new BobBufferPool())
              .socketPoolProvider(new BongoSocketPoolProviderVibur())
              .codec(new BongoCodecBobBson(bobBson))
              .build();

      var bongo = new BongoClient(settings);
      bongo.connect();

      var database = bongo.getDatabase("test_data");
      var collection = database.getCollection("people4", Person.class);

      this.concurrentStrategy =
          new ReadExecutionConcurrentStrategy<Person>(
              settings.getCodec().converter(Person.class), 1);
      this.bongoClient = bongo;
      this.bongoDatabase = database;
      this.collection = collection;
      this.codec = settings.getCodec();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      bongoClient.close();
      this.concurrentStrategy.close();
    }
  }

  @State(Scope.Benchmark)
  public static class MyMongoClient {
    public MongoClient client;
    public MongoDatabase mongoDatabase;
    public MongoCollection<Person> mongoCollection;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
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
                  new ConnectionString("mongodb://192.168.1.138:27027,192.168.1.138:27028,192.168.1.138:27029/"))
              .build();
      client = MongoClients.create(settings);
      mongoDatabase = client.getDatabase("test_data");
      mongoCollection = mongoDatabase.getCollection("people4", Person.class);
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      System.out.println("IS THIS RUNNING?");
      mongoCollection.drop();
      client.close();
    }
  }

  private List<Person> people;

  @Setup
  public void setup() {

    Faker faker = new Faker(new Random(23));
    people = new ArrayList<>();
    for (var i = 0; i < 10000; i+= 1) {
      var person = new Person();
      person.setName(faker.name().fullName());
      person.setAge(faker.number().numberBetween(1, 99));
      person.setOccupation(faker.job().position());
      person.setAddress(faker.lorem().characters(1200));
      person.setDescription(Arrays.asList(faker.color().hex(), faker.color().hex()));
      var scores = new Scores();
      scores.setScore1(faker.number().randomDouble(2, 0, 1000));
      scores.setScore2(faker.number().randomDouble(2, 0, 1000));
      scores.setScore3(faker.number().randomDouble(2, 0, 1000));
      scores.setScore4(faker.number().randomDouble(2, 0, 1000));
      scores.setScore5(faker.number().randomDouble(2, 0, 1000));
      scores.setScore6(faker.number().randomDouble(2, 0, 1000));
      scores.setScore7(faker.number().randomDouble(2, 0, 1000));
      person.setScores(scores);
      people.add(person);
    }
  }

  @Benchmark
  public void bongo(Blackhole bh, MyBongoClient state) {
    state.collection.insertMany(people, new WriteExecutionSerialStrategy<>(), false);
  }

//  @Benchmark
//  public void bongoCompression(Blackhole bh, MyBongoClient state) {
//    state.collection.insertMany(people, new WriteExecutionSerialStrategy<>(), true);
//  }

  @Benchmark
  public void mongoCompression(Blackhole bh, MyMongoClient state) {
    state.mongoCollection.insertMany(people);
  }
}
