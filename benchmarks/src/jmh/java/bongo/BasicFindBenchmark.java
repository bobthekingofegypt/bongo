package bongo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.*;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.ReadExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.ReadExecutionSerialStrategy;
import org.bobstuff.bongo.models.Person;
import org.bobstuff.bongo.models.Scores;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 20000, timeUnit = MILLISECONDS)
public class BasicFindBenchmark {
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
      var collection = database.getCollection("people", Person.class);

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
                  new ConnectionString("mongodb://192.168.1.138:27027/?compressors=zstd"))
              .build();
      client = MongoClients.create(settings);
      mongoDatabase = client.getDatabase("test_data");
      mongoCollection = mongoDatabase.getCollection("people", Person.class);
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      client.close();
    }
  }

  @Benchmark
  public void bongoFindNoLimitNoCompressionNoExhaust(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<Person>(state.codec.converter(Person.class)))
            .compress(false)
            .cursorType(BongoCursorType.Default)
            .cursor();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    System.out.println(i);
  }

  @Benchmark
  public void bongoFindNoLimitCompressionNoExhaust(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<Person>(state.codec.converter(Person.class)))
            .compress(true)
            .cursorType(BongoCursorType.Default)
            .cursor();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    System.out.println(i);
  }

  @Benchmark
  public void bongoFindNoLimitNoCompression(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<Person>(state.codec.converter(Person.class)))
            .cursorType(BongoCursorType.Exhaustible)
            .compress(false)
            .cursor();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    System.out.println(i);
  }

  @Benchmark
  public void bongoFindNoLimit(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(
                null,
                null,
                new ReadExecutionSerialStrategy<Person>(state.codec.converter(Person.class)))
            .cursorType(BongoCursorType.Exhaustible)
            .cursor();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    System.out.println(i);
  }

  @Benchmark
  public void bongoFindNoLimitConcurrent_CE(Blackhole bh, MyBongoClient state) {
    var iter =
        state
            .collection
            .find(null, null, state.concurrentStrategy)
            .cursorType(BongoCursorType.Exhaustible)
            .cursor();

    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i += 1;
    }

    System.out.println(i);
  }

  @Benchmark
  public void mongoFindNoLimit(Blackhole bh, MyMongoClient state) {
    int i;
    try (var iter = state.mongoCollection.find().iterator()) {

      i = 0;
      while (iter.hasNext()) {
        iter.next();
        i += 1;
      }
    }

    System.out.println(i);
  }
}