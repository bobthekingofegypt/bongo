package bongo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import net.datafaker.Faker;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.models.Person;
import org.bobstuff.bongo.models.Scores;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 10000, timeUnit = MILLISECONDS)
public class CompanyInsertBenchmark {
  @State(Scope.Benchmark)
  public static class MyBongoClient {
    public BongoClient bongoClient;
    public BongoDatabase bongoDatabase;
    public BongoCollection<Company> collection;

    public BongoCodec codec;

    public ReadExecutionConcurrentStrategy<Company> concurrentStrategy;

    public WriteExecutionConcurrentStrategy<Company> writeConcurrentStrategy;
    public WriteExecutionConcurrentStrategy<Company> writeConcurrentStrategy3;

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
      var collection = database.getCollection("companies4", Company.class);

      this.writeConcurrentStrategy = new WriteExecutionConcurrentStrategy<Company>(3, 1);
      this.writeConcurrentStrategy3 = new WriteExecutionConcurrentStrategy<Company>(3, 5);
      this.concurrentStrategy =
          new ReadExecutionConcurrentStrategy<Company>(
              1);
      this.bongoClient = bongo;
      this.bongoDatabase = database;
      this.collection = collection;
      this.codec = settings.getCodec();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      bongoClient.close();
      this.concurrentStrategy.close();
      this.writeConcurrentStrategy.close();
      this.writeConcurrentStrategy3.close();
    }
  }

  @State(Scope.Benchmark)
  public static class MyMongoClientCompression {
    public MongoClient client;
    public MongoDatabase mongoDatabase;
    public MongoCollection<Company> mongoCollection;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      PojoCodecProvider.Builder providerBuilder = PojoCodecProvider.builder();
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
                      "mongodb://192.168.1.138:27027,192.168.1.138:27028,192.168.1.138:27029/?compressors=zstd"))
              .build();
      client = MongoClients.create(settings);
      mongoDatabase = client.getDatabase("test_data");
      mongoCollection = mongoDatabase.getCollection("companies4", Company.class);
    }
  }

  @State(Scope.Benchmark)
  public static class MyMongoClient {
    public MongoClient client;
    public MongoDatabase mongoDatabase;
    public MongoCollection<Company> mongoCollection;

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
                  new ConnectionString(
                      "mongodb://192.168.1.138:27027,192.168.1.138:27028,192.168.1.138:27029/"))
              .build();
      client = MongoClients.create(settings);
      mongoDatabase = client.getDatabase("test_data");
      mongoCollection = mongoDatabase.getCollection("companies4", Company.class);
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      System.out.println("IS THIS RUNNING?");
      mongoCollection.drop();
      client.close();
    }
  }

  private List<Company> companies;

  @Setup
  public void setup() {

    Faker faker = new Faker(new Random(23));
    companies = new ArrayList<>();
    for (var i = 0; i < 100000; i += 1) {
      companies.add(CompanyDataGenerator.company(faker));
    }
  }

  @Benchmark
  public void bongo(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
    state.collection.insertMany(
        companies,
        new WriteExecutionSerialStrategy<>(),
        BongoInsertManyOptions.builder().compress(false).build());
  }

  @Benchmark
  public void bongoUnordered(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
    state.collection.insertMany(
        companies,
        new WriteExecutionSerialStrategy<>(),
        BongoInsertManyOptions.builder().ordered(false).compress(false).build());
  }

  @Benchmark
  public void bongoUnackn(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
    state
        .collection
        .withWriteConcern(new BongoWriteConcern(0, false))
        .insertMany(
            companies,
            new WriteExecutionSerialStrategy<>(),
            BongoInsertManyOptions.builder().ordered(false).compress(false).build());
  }

  @Benchmark
  public void bongoUnacknConcRe(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
    state
        .collection
        .withWriteConcern(new BongoWriteConcern(0, false))
        .insertMany(
            companies,
            state.writeConcurrentStrategy3,
            BongoInsertManyOptions.builder().ordered(false).compress(false).build());
  }

  @Benchmark
  public void bongoUnacknConc(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
    var strategy = new WriteExecutionConcurrentStrategy<Company>(3, 5);
    state
        .collection
        .withWriteConcern(new BongoWriteConcern(0, false))
        .insertMany(
            companies,
            strategy,
            BongoInsertManyOptions.builder().ordered(false).compress(false).build());
    strategy.close();
  }

  //  @Benchmark
  //  public void bongoCompression(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
  //    state.collection.insertMany(people, new WriteExecutionSerialStrategy<>(), true);
  //  }
  //
  //  @Benchmark
  //  public void bongo3writ2sen(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
  //    var s = new WriteExecutionConcurrentStrategy<Person>(3, 2);
  //    state.collection.insertMany(people, s, false);
  //    s.close();
  //  }
  //
  //  @Benchmark
  //  public void bongoCompression3writ2sen(Blackhole bh, MyBongoClient state, MyMongoClient mongo)
  // {
  //    var s = new WriteExecutionConcurrentStrategy<Person>(3, 2);
  //    state.collection.insertMany(people, s, true);
  //    s.close();
  //  }
  //
  //  @Benchmark
  //  public void bongo3writ(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
  //    var s = new WriteExecutionConcurrentStrategy<Person>(3, 1);
  //    state.collection.insertMany(people, s, false);
  //    s.close();
  //  }
  //
  //  @Benchmark
  //  public void bongoCompression3writ(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
  //    var s = new WriteExecutionConcurrentStrategy<Person>(3, 1);
  //    state.collection.insertMany(people, s, true);
  //    s.close();
  //  }
  //
  //  @Benchmark
  //  public void bongoCompression3writRe(Blackhole bh, MyBongoClient state, MyMongoClient mongo) {
  //    state.collection.insertMany(people, state.writeConcurrentStrategy, true);
  //  }

  @Benchmark
  public void mongo(Blackhole bh, MyMongoClient state) {
    state.mongoCollection.insertMany(companies);
  }

  @Benchmark
  public void mongoUnorderedNoack(Blackhole bh, MyMongoClient state) {
    state
        .mongoCollection
        .withWriteConcern(WriteConcern.UNACKNOWLEDGED)
        .insertMany(companies, new InsertManyOptions().ordered(false));
  }

  //  @Benchmark
  //  public void mongoCompression(Blackhole bh, MyMongoClientCompression state) {
  //    state.mongoCollection.insertMany(people);
  //  }
}
