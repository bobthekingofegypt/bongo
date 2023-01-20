package org.bobstuff.bongo.models.company;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.CompiledBson;

@Builder
@CompiledBson
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Address {
  private String street;
  private String town;
  private String country;
  private String postCode;
}
