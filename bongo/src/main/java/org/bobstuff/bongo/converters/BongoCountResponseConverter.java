// package org.bobstuff.bongo.converters;
//
// import org.bobstuff.bobbson.BobBsonBuffer;
// import org.bobstuff.bobbson.BobBsonConverter;
// import org.bobstuff.bobbson.BsonReader;
// import org.bobstuff.bongo.messages.BongoFindResponse;
// import org.checkerframework.checker.initialization.qual.Initialized;
// import org.checkerframework.checker.nullness.qual.NonNull;
// import org.checkerframework.checker.nullness.qual.Nullable;
// import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
//
// import java.nio.charset.StandardCharsets;
// import java.util.ArrayList;
//
// public class BongoCountResponseConverter<TModel>
//    implements BobBsonConverter<BongoFindResponse<TModel>> {
//  private static byte[] okKey = "ok".getBytes(StandardCharsets.UTF_8);
//  private static byte[] nKey = "n".getBytes(StandardCharsets.UTF_8);
//  private static byte[] errorMessageKey = "errmsg".getBytes(StandardCharsets.UTF_8);
//
//  private static int okHash;
//  private static int nHash;
//  private static int errorMessageHash;
//
//  static {
//    int i = 0;
//    for (byte b : okKey) {
//      i += b;
//    }
//    okHash = i;
//    for (byte b : nKey) {
//      i += b;
//    }
//    nHash = i;
//    i = 0;
//    for (byte b : errorMessageKey) {
//      i += b;
//    }
//    errorMessageHash = i;
//  }
//
//  public BongoCountResponseConverter() {
//  }
//
//  @Override
//  public @Nullable BongoFindResponse<TModel> read(
//      @UnknownKeyFor @NonNull @Initialized BsonReader reader) {
//    var readResponse = new BongoFindResponse<TModel>();
//
//    reader.readStartDocument();
//    BobBsonBuffer.ByteRangeComparitor range = reader.getFieldName();
//    while (reader.readBsonType() != org.bobstuff.bobbson.BsonType.END_OF_DOCUMENT) {
//      if (range.equalsArray(okKey, okHash)) {
//        readResponse.setOk(reader.readDouble());
//      } else if (range.equalsArray(errorMessageKey, errorMessageHash)) {
//        readResponse.setErrmsg(reader.readString());
//      } else if (range.equalsArray(nKey, nHash)) {
//        readResponse.setN(reader.readString());
//      } else {
//        reader.skipValue();
//      }
//    }
//
//    return readResponse;
//  }
// }
