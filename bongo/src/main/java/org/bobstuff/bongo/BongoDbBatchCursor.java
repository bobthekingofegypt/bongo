package org.bobstuff.bongo;

import java.util.Iterator;
import java.util.List;

public interface BongoDbBatchCursor<TModel> extends Iterator<List<TModel>>, AutoCloseable {}
