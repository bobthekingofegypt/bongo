package org.bobstuff.bongo.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.bobstuff.bobbson.annotations.GenerateBobBsonConverter;

@GenerateBobBsonConverter
@Builder
@Data
@AllArgsConstructor
public class Scores {
  private Double score1;
  private Double score2;
  private Double score3;
  private Double score4;
  private Double score5;
  private Double score6;
  private Double score7;

  public Scores() {}

  public Double getScore1() {
    return score1;
  }

  public void setScore1(Double score1) {
    this.score1 = score1;
  }

  public Double getScore2() {
    return score2;
  }

  public void setScore2(Double score2) {
    this.score2 = score2;
  }

  public Double getScore3() {
    return score3;
  }

  public void setScore3(Double score3) {
    this.score3 = score3;
  }

  public Double getScore4() {
    return score4;
  }

  public void setScore4(Double score4) {
    this.score4 = score4;
  }

  public Double getScore5() {
    return score5;
  }

  public void setScore5(Double score5) {
    this.score5 = score5;
  }

  public Double getScore6() {
    return score6;
  }

  public void setScore6(Double score6) {
    this.score6 = score6;
  }

  public Double getScore7() {
    return score7;
  }

  public void setScore7(Double score7) {
    this.score7 = score7;
  }
}
