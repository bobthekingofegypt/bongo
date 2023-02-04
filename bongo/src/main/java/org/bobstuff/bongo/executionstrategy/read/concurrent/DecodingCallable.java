package org.bobstuff.bongo.executionstrategy.read.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.BufferDataPool;
import org.bobstuff.bobbson.buffer.BobBufferBobBsonBuffer;
import org.bobstuff.bongo.converters.BongoFindResponseConverter;
import org.bobstuff.bongo.messages.BongoFindResponse;

@Slf4j
public class DecodingCallable<TModel> implements Callable<Void> {
  public static final BobBsonBuffer POISON_BUFFER = new BobBufferBobBsonBuffer(new byte[0]);
  public static final BobBsonBuffer ABORTED_BUFFER = new BobBufferBobBsonBuffer(new byte[0]);
  private BlockingQueue<BobBsonBuffer> decodeQueue;
  private BongoFindResponseConverter<TModel> findResponseConverter;
  private BlockingQueue<BongoFindResponse<TModel>> responses;
  private BufferDataPool bufferPool;

  public DecodingCallable(
      BlockingQueue<BobBsonBuffer> decodeQueue,
      BlockingQueue<BongoFindResponse<TModel>> responses,
      BongoFindResponseConverter<TModel> findResponseConverter,
      BufferDataPool bufferPool) {
    this.decodeQueue = decodeQueue;
    this.findResponseConverter = findResponseConverter;
    this.bufferPool = bufferPool;
    this.responses = responses;
  }

  @Override
  public Void call() throws Exception {
    try {
      while (true) {
        BobBsonBuffer buffer;
        try {
          buffer = decodeQueue.take();
        } catch (InterruptedException e) {
          log.debug("Decoder interrupted while waiting on tasks");
          break;
        }
        if (buffer == POISON_BUFFER) {
          log.debug("poison decode entry found");
          break;
        }
        log.debug("decoding a new entry");
        buffer.setHead(5);
        BsonReader reader = new BsonReader(buffer);
        var result = findResponseConverter.read(reader);

        bufferPool.recycle(buffer);
        if (result != null) {
          try {
            responses.put(result);
          } catch (InterruptedException e) {
            log.debug(
                "Decoder thread interrupted waiting for space to put decoded response on queue");
            break;
          }
        }
      }
    } catch (Exception e) {
      //      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return null;
  }
}
