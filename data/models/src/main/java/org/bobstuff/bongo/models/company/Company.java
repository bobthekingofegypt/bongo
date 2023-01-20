package org.bobstuff.bongo.models.company;

import java.time.Instant;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.BsonConverter;
import org.bobstuff.bobbson.annotations.BsonWriterOptions;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bobstuff.bongo.models.InstantConverter;
import org.bobstuff.bongo.models.Person;
import org.bson.types.ObjectId;

@CompiledBson
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Company {
  @BsonAttribute(value = "_id", order = 14)
  @BsonWriterOptions(writeNull = false)
  private ObjectId mongoId;

  private String name;
  private String description;
  private Address address;

  @BsonConverter(target = InstantConverter.class)
  private Instant foundedDate;

  private Stats stats;
  private Person owner;
  private int reviewScore;
  private List<Review> reviews;
}
