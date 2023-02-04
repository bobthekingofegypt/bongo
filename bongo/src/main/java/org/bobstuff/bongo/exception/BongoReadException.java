package org.bobstuff.bongo.exception;

public class BongoReadException extends BongoException {
  private int code;
  private String errorMesage;

  private String codeName;

  public BongoReadException(int code, String errorMesage, String codeName) {
    super(errorMesage);
    this.code = code;
    this.errorMesage = errorMesage;
    this.codeName = codeName;
  }

  @Override
  public String toString() {
    return "BongoReadException{"
        + "code="
        + code
        + ", errorMesage='"
        + errorMesage
        + '\''
        + ", codeName='"
        + codeName
        + '\''
        + '}';
  }
}
