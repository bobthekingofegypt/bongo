package org.bobstuff.bongo.converters;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.BsonType;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.reader.BsonReader;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoFindResponseConverter<TModel>
    implements BobBsonConverter<BongoFindResponse<TModel>> {
  private static byte[] cursorKey = "cursor".getBytes(StandardCharsets.UTF_8);
  private static byte[] idKey = "id".getBytes(StandardCharsets.UTF_8);
  private static byte[] nextBatchKey = "nextBatch".getBytes(StandardCharsets.UTF_8);
  private static byte[] firstBatchKey = "firstBatch".getBytes(StandardCharsets.UTF_8);
  private static byte[] okKey = "ok".getBytes(StandardCharsets.UTF_8);
  private static byte[] errorMessageKey = "errmsg".getBytes(StandardCharsets.UTF_8);

  private static int cursorHash;
  private static int idHash;
  private static int nextBatchHash;
  private static int firstBatchHash;
  private static int okHash;
  private static int errorMessageHash;

  static {
    int i = 0;
    for (byte b : cursorKey) {
      i += b;
    }
    cursorHash = i;
    i = 0;
    for (byte b : idKey) {
      i += b;
    }
    idHash = i;
    i = 0;
    for (byte b : nextBatchKey) {
      i += b;
    }
    nextBatchHash = i;
    i = 0;
    for (byte b : firstBatchKey) {
      i += b;
    }
    firstBatchHash = i;
    i = 0;
    for (byte b : okKey) {
      i += b;
    }
    okHash = i;
    i = 0;
    for (byte b : errorMessageKey) {
      i += b;
    }
    errorMessageHash = i;
  }

  private boolean skipBody;
  private BobBsonConverter<TModel> converter;

  public BongoFindResponseConverter(BobBsonConverter<TModel> converter, boolean skipBody) {
    this.skipBody = skipBody;
    this.converter = converter;
  }

  @Override
  public @Nullable BongoFindResponse<TModel> read(
      @UnknownKeyFor @NonNull @Initialized BsonReader reader) {
    var readResponse = new BongoFindResponse<TModel>();

    reader.readStartDocument();
    BobBsonBuffer.ByteRangeComparator range = reader.getFieldName();
    while (reader.readBsonType() != org.bobstuff.bobbson.BsonType.END_OF_DOCUMENT) {
      if (range.equalsArray(cursorKey, cursorHash)) {
        reader.readStartDocument();
        while (reader.readBsonType() != org.bobstuff.bobbson.BsonType.END_OF_DOCUMENT) {
          if (range.equalsArray(idKey, idHash)) {
            readResponse.setId(reader.readInt64());
          } else if (range.equalsArray(nextBatchKey, nextBatchHash)
              || range.equalsArray(firstBatchKey, firstBatchHash)) {
            if (skipBody) {
              reader.skipValue();
            } else {
              var batch = new ArrayList<TModel>();
              reader.readStartArray();
              while (reader.readBsonType() != org.bobstuff.bobbson.BsonType.END_OF_DOCUMENT) {
                var value = converter.read(reader);
                if (value != null) {
                  batch.add(value);
                }
              }
              reader.readEndArray();
              readResponse.setBatch(batch);
            }
          } else {
            reader.skipValue();
          }
        }
        reader.readEndDocument();
      } else if (range.equalsArray(okKey, okHash)) {
        readResponse.setOk(reader.readDouble());
      } else if (range.equalsArray(errorMessageKey, errorMessageHash)) {
        readResponse.setErrmsg(reader.readString());
      } else {
        reader.skipValue();
      }
    }

    return readResponse;
  }

  @Override
  public @Nullable BongoFindResponse<TModel> readValue(
      @UnknownKeyFor @NonNull @Initialized BsonReader bsonReader,
      @UnknownKeyFor @NonNull @Initialized BsonType type) {
    throw new UnsupportedOperationException("method not used");
  }

  @Override
  public void writeValue(
      @UnknownKeyFor @NonNull @Initialized BsonWriter bsonWriter, BongoFindResponse<TModel> value) {
    throw new UnsupportedOperationException("method not used");
  }
}
