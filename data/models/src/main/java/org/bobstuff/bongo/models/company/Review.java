package org.bobstuff.bongo.models.company;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.BsonConverter;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bobstuff.bongo.models.InstantConverter;

@Data
@Builder
@CompiledBson
@NoArgsConstructor
@AllArgsConstructor
public class Review {
  private int stars;
  private String text;
  private String title;
  @BsonConverter(target = InstantConverter.class)
  private Instant postedAt;
}
