package org.bobstuff.bongo;

import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.ContextStack;
import org.bobstuff.bobbson.Decimal128;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bson.types.ObjectId;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BongoBsonWriterId extends BsonWriter {
    private int contextLevel;
    private static String ID_KEY = "_id";
    private static byte[] ID_KEY_BYTES = "_id".getBytes();

    private boolean hasWrittenId;
    private @Nullable String lastName;
    private byte @Nullable [] writtenId;

    public BongoBsonWriterId(@UnknownKeyFor @NonNull @Initialized BufferDataPool bufferDataPool) {
        super(bufferDataPool);
    }

    public BongoBsonWriterId(@UnknownKeyFor @NonNull @Initialized BufferDataPool bufferDataPool,
                             @UnknownKeyFor @NonNull @Initialized ContextStack contextStack) {
        super(bufferDataPool, contextStack);
    }

    public BongoBsonWriterId(@UnknownKeyFor @NonNull @Initialized BobBsonBuffer buffer) {
        super(buffer);
    }

    public BongoBsonWriterId(@UnknownKeyFor @NonNull @Initialized BobBsonBuffer buffer,
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
        super.writeName(name);
    }

    @Override
    public void writeName(byte @NonNull [] name) {
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
        if (contextLevel == 1 && ID_KEY.equals(lastName)) {
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
