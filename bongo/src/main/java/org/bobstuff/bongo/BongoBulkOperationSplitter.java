package org.bobstuff.bongo;

import org.bobstuff.bobbson.BobBsonBuffer;

public interface BongoBulkOperationSplitter {
    public BongoWriteOperationType nextType();
    void write(BobBsonBuffer buffer);
    boolean hasMore();
}
