package org.bobstuff.bongo.models.company;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@GenerateBobBsonConverter
public class Stats {
  private double revenue;
  private double profit;
  private double payroll;
  private int staffCount;
  private int contractorCount;
}
