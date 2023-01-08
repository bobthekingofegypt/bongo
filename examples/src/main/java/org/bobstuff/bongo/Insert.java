package org.bobstuff.bongo;

import com.google.common.base.Stopwatch;
import org.bobstuff.bobbson.BobBson;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.converters.BsonValueConverters;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.codec.BongoCodecBobBson;
import org.bobstuff.bongo.compressors.BongoCompressorZstd;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.models.Person;
import org.bobstuff.bongo.vibur.BongoSocketPoolProviderVibur;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    var collection = database.getCollection("people3", Person.class);

    Person person = new Person();
    person.setAddress("94 Somewhere");
    person.setName("a name");
    person.setAge(34);
    person.setDescription(List.of("fdafkj dfajlfkdjafl jfaldfj"));

    var people = new ArrayList<Person>();
    for (var i = 0; i < 1000000; i+= 1) {
      people.add(person);
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    collection.insertMany(people, new WriteExecutionSerialStrategy<>(), true);

    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));

    bongo.close();
  }
}
