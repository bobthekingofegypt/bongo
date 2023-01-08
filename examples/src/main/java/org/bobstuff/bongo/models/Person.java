package org.bobstuff.bongo.models;

import java.util.List;
import org.bobstuff.bobbson.annotations.BsonAttribute;
import org.bobstuff.bobbson.annotations.BsonWriterOptions;
import org.bobstuff.bobbson.annotations.CompiledBson;
import org.bson.types.ObjectId;

@CompiledBson
public class Person {
  @BsonAttribute(value = "_id", order = 14)
  @BsonWriterOptions(writeNull = false)
  private ObjectId mongoId;

  @BsonAttribute(value = "name", order = 10)
  private String name;

  @BsonAttribute(value = "age", order = 2)
  private int age;

  @BsonAttribute(value = "occupation", order = 12)
  private String occupation;

  @BsonAttribute(value = "address", order = 1)
  private String address;

  @BsonAttribute(value = "description", order = 3)
  private List<String> description;

  @BsonAttribute(value = "occupation", order = 13)
  private Scores scores;

  public ObjectId getMongoId() {
    return mongoId;
  }

  public void setMongoId(ObjectId mongoId) {
    this.mongoId = mongoId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public String getOccupation() {
    return occupation;
  }

  public void setOccupation(String occupation) {
    this.occupation = occupation;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public List<String> getDescription() {
    return description;
  }

  public void setDescription(List<String> description) {
    this.description = description;
  }

  public Scores getScores() {
    return scores;
  }

  public void setScores(Scores scores) {
    this.scores = scores;
  }

  @Override
  public String toString() {
    return "Person{"
        + "mongoId="
        + mongoId
        + ", name='"
        + name
        + '\''
        + ", age="
        + age
        + ", occupation='"
        + occupation
        + '\''
        + ", address='"
        + address
        + '\''
        + ", description="
        + description
        + ", scores="
        + scores
        + '}';
  }
}
