package bongo;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.pool.ConcurrentBobBsonBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = MINUTES)
public class BasicFindCompanyBenchmark {
  @State(Scope.Benchmark)
  public static class MyBongoClient {
    public BongoClient bongoClient;
    public BongoDatabase bongoDatabase;
    public BongoCollection<Company> collection;

    public BongoCodec codec;

    public ReadExecutionConcurrentStrategy<Company> concurrentStrategy;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      var bobBson = new BobBson();
      BsonValueConverters.register(bobBson);
      bobBson.registerConverter(ObjectId.class, new BongoObjectIdConverter());

      var settings =
          BongoSettings.builder()
              .connectionSettings(
                  BongoConnectionSettings.builder()
                      .host("192.168.1.139:27017")
                      .compressor(new BongoCompressorZstd())
                      .credentials(
                          BongoCredentials.builder()
                              .authSource("admin")
                              .username("admin")
                              .password("speal")
                              .build())
                      .build())
              .bufferPool(new ConcurrentBobBsonBufferPool())
              .socketPoolProvider(new BongoSocketPoolProviderVibur())
              .codec(new BongoCodecBobBson(bobBson))
              .build();

      var bongo = new BongoClient(settings);
      bongo.connect();

      var database = bongo.getDatabase("test_data");
      var collection = database.getCollection("companies", Company.class);

      this.concurrentStrategy = new ReadExecutionConcurrentStrategy<Company>(3);
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
                      "mongodb://admin:speal@192.168.1.139:27017/?authSource=admin"))
              .build();
      client = MongoClients.create(settings);
      mongoDatabase = client.getDatabase("test_data");
      mongoCollection = mongoDatabase.getCollection("companies", Company.class);
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      client.close();
    }
  }

  @State(Scope.Benchmark)
  public static class MyMongoClientComp {
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
                      "mongodb://admin:speal@192.168.1.139:27017/?authSource=admin&compressors=zstd"))
              .build();
      client = MongoClients.create(settings);
      mongoDatabase = client.getDatabase("test_data");
      mongoCollection = mongoDatabase.getCollection("companies", Company.class);
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      client.close();
    }
  }

  @Benchmark
  public void bongoNoExhaust(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(new ReadExecutionSerialStrategy<Company>())
            .compress(false)
            .cursorType(BongoCursorType.Default)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void bongoCompressNoExhaustConc3_Re(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(state.concurrentStrategy)
            .compress(true)
            .cursorType(BongoCursorType.Default)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void bongoCompressNoExhaust(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(new ReadExecutionSerialStrategy<Company>())
            .compress(true)
            .cursorType(BongoCursorType.Default)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void bongoConc3_Re(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(state.concurrentStrategy)
            .cursorType(BongoCursorType.Exhaustible)
            .compress(false)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }
  }

  @Benchmark
  public void bongoConc1(Blackhole bh, MyBongoClient state) {
    var strategy = new ReadExecutionConcurrentStrategy<Company>(1);
    var iter =
        state
            .collection
            .find(strategy)
            .cursorType(BongoCursorType.Exhaustible)
            .compress(false)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    strategy.close();
  }

  @Benchmark
  public void bongoConc3(Blackhole bh, MyBongoClient state) {
    var strategy = new ReadExecutionConcurrentStrategy<Company>(3);
    var iter =
        state
            .collection
            .find(strategy)
            .cursorType(BongoCursorType.Exhaustible)
            .compress(false)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }
    strategy.close();
  }

  @Benchmark
  public void bongo(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(new ReadExecutionSerialStrategy<Company>())
            .cursorType(BongoCursorType.Exhaustible)
            .compress(false)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void bongoComp(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(new ReadExecutionSerialStrategy<Company>())
            .cursorType(BongoCursorType.Exhaustible)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void bongoCompExhaustConcurrent_Re(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(state.concurrentStrategy)
            .cursorType(BongoCursorType.Exhaustible)
            .iterator();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void mongo(Blackhole bh, MyMongoClient state) {
    int i;
    try (var iter = state.mongoCollection.find().iterator()) {

      i = 0;
      while (iter.hasNext()) {
        iter.next();
        i += 1;
      }
    }

    //    System.out.println(i);
  }

  @Benchmark
  public void mongoComp(Blackhole bh, MyMongoClientComp state) {
    int i;
    try (var iter = state.mongoCollection.find().iterator()) {

      i = 0;
      while (iter.hasNext()) {
        iter.next();
        i += 1;
      }
    }

    //    System.out.println(i);
  }
}
