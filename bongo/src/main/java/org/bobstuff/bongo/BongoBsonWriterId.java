package org.bobstuff.bongo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.bobstuff.bobbson.ContextStack;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.writer.StackBsonWriter;
import org.bson.types.ObjectId;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BongoBsonWriterId extends StackBsonWriter {
  private int contextLevel;
  private static String ID_KEY = "_id";
  private static byte[] ID_KEY_BYTES = "_id".getBytes();

  private boolean hasWrittenId;
  private @Nullable String lastName;
  private byte @Nullable [] lastNameBytes;
  private byte @Nullable [] writtenId;

  public BongoBsonWriterId(@UnknownKeyFor @NonNull @Initialized BobBsonBuffer buffer) {
    super(buffer);
  }

  public BongoBsonWriterId(
      @UnknownKeyFor @NonNull @Initialized BobBsonBuffer buffer,
      @UnknownKeyFor @NonNull @Initialized ContextStack contextStack) {
    super(buffer, contextStack);
  }

  public void reset() {
    hasWrittenId = false;
    lastName = null;
    writtenId = null;
  }

  public byte @Nullable [] getWrittenId() {
    return writtenId;
  }

  @Override
  public void writeStartDocument() {
    contextLevel += 1;
    super.writeStartDocument();
  }

  @Override
  public void writeStartDocument(byte @NonNull [] field) {
    contextLevel += 1;
    super.writeStartDocument(field);
  }

  @Override
  public void writeStartDocument(@NonNull String name) {
    contextLevel += 1;
    super.writeStartDocument(name);
  }

  @Override
  public void writeNull(@NonNull String field) {
    this.writeNull(field.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void writeNull(byte @NonNull [] field) {
    if (contextLevel == 1 && Arrays.equals(ID_KEY_BYTES, field)) {
      hasWrittenId = true;
      writtenId = null;
    }
    super.writeNull(field);
  }

  @Override
  public void writeNull() {
    if (contextLevel == 1 && ID_KEY.equals(lastName)) {
      hasWrittenId = true;
      writtenId = null;
    }
    super.writeNull();
  }

  @Override
  public void writeEndDocument() {
    contextLevel -= 1;
    if (contextLevel == 0 && !hasWrittenId) {
      var id = new ObjectId();
      writtenId = id.toByteArray();
      this.writeObjectId(ID_KEY_BYTES, writtenId);
    }
    super.writeEndDocument();
  }

  @Override
  public void writeName(@NonNull String name) {
    lastName = name;
    lastNameBytes = null;
    super.writeName(name);
  }

  @Override
  public void writeName(byte @NonNull [] name) {
    lastName = null;
    lastNameBytes = name;
    super.writeName(name);
  }

  @Override
  public void writeObjectId(byte @NonNull [] key, byte @NonNull [] value) {
    if (contextLevel == 1 && Arrays.equals(ID_KEY_BYTES, key)) {
      hasWrittenId = true;
      writtenId = value;
    }
    super.writeObjectId(key, value);
  }

  @Override
  public void writeObjectId(byte @NonNull [] value) {
    if (contextLevel == 1
        && ((lastNameBytes != null && Arrays.equals(ID_KEY_BYTES, lastNameBytes))
            || (ID_KEY.equals(lastName)))) {
      hasWrittenId = true;
      writtenId = value;
    }
    super.writeObjectId(value);
  }

  @Override
  public void writeObjectId(@NonNull String field, byte @NonNull [] value) {
    this.writeObjectId(field.getBytes(StandardCharsets.UTF_8), value);
  }

  @Override
  public void writeStartArray(byte @NonNull [] field) {
    contextLevel += 1;
    super.writeStartArray(field);
  }

  @Override
  public void writeStartArray(@NonNull String field) {
    contextLevel += 1;
    super.writeStartArray(field);
  }

  @Override
  public void writeStartArray() {
    contextLevel += 1;
    super.writeStartArray();
  }
}
