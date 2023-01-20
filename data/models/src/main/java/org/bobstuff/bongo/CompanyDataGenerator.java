package org.bobstuff.bongo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import net.datafaker.Faker;
import org.bobstuff.bongo.models.Person;
import org.bobstuff.bongo.models.Scores;
import org.bobstuff.bongo.models.company.Address;
import org.bobstuff.bongo.models.company.Company;
import org.bobstuff.bongo.models.company.Review;
import org.bobstuff.bongo.models.company.Stats;

public class CompanyDataGenerator {
  public static Company company(Faker faker) {
    var numberOfReviews = faker.number().numberBetween(0, 3);
    var reviews = new ArrayList<Review>();
    for (var x = 0; x < numberOfReviews; x += 1) {
      var review =
          Review.builder()
              .stars(faker.number().numberBetween(0, 10))
              .title(faker.lorem().sentence())
              .text(faker.lorem().sentence(faker.number().numberBetween(10, 1000)))
              .postedAt(
                  faker
                      .date()
                      .between(
                          Date.from(Instant.parse("2007-03-01T13:00:00Z")),
                          Date.from(Instant.parse("2017-03-01T13:00:00Z")))
                      .toInstant())
              .build();
      reviews.add(review);
    }
    return Company.builder()
        .name(faker.name().fullName())
        .description(faker.lorem().sentence(faker.number().numberBetween(50, 300)))
        .address(
            Address.builder()
                .street(faker.address().streetAddress())
                .town(faker.address().city())
                .country(faker.address().country())
                .postCode(faker.address().postcode())
                .build())
        .foundedDate(
            faker
                .date()
                .between(
                    Date.from(Instant.parse("2007-03-01T13:00:00Z")),
                    Date.from(Instant.parse("2017-03-01T13:00:00Z")))
                .toInstant())
        .stats(
            Stats.builder()
                .payroll(faker.number().randomDouble(8, 10000, 10000000))
                .payroll(faker.number().randomDouble(8, 10000, 1000000))
                .contractorCount(faker.number().numberBetween(1, 100))
                .staffCount(faker.number().numberBetween(10, 100))
                .profit(faker.number().randomDouble(8, 10000, 1000000))
                .revenue(faker.number().randomDouble(8, 10000, 1000000))
                .build())
        .reviewScore(faker.number().numberBetween(0, 10))
        .owner(
            Person.builder()
                .age(faker.number().numberBetween(18, 80))
                .address(faker.address().fullAddress())
                .occupation(faker.job().field())
                .description(faker.lorem().paragraphs(faker.number().numberBetween(1, 8)))
                .name(faker.name().fullName())
                .scores(
                    Scores.builder()
                        .score1(faker.number().randomDouble(8, 100, 10000))
                        .score2(faker.number().randomDouble(8, 100, 10000))
                        .score3(faker.number().randomDouble(8, 100, 10000))
                        .score4(faker.number().randomDouble(8, 100, 10000))
                        .score5(faker.number().randomDouble(8, 100, 10000))
                        .score6(faker.number().randomDouble(8, 100, 10000))
                        .score7(faker.number().randomDouble(8, 100, 10000))
                        .build())
                .build())
        .reviews(reviews)
        .build();
  }
}
