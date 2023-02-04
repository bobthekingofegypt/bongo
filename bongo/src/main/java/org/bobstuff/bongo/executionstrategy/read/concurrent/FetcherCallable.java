package org.bobstuff.bongo.executionstrategy.read.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.bobstuff.bobbson.*;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.converters.BongoFindResponseConverter;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.bobstuff.bongo.messages.BongoGetMoreRequest;

@Slf4j
public class FetcherCallable<TModel> implements Callable<Void> {
  private boolean exhaustible;
  private BongoGetMoreRequest getMoreRequest;
  private BongoSocket socket;
  private WireProtocol wireProtocol;
  private boolean requestCompression;
  private BongoFindResponseConverter<TModel> findResponseConverterSkipBody;
  private BobBsonConverter<BongoGetMoreRequest> getMoreRequestConverter;
  private BlockingQueue<BobBsonBuffer> decodeQueue;

  public FetcherCallable(
      boolean exhaustible,
      BongoGetMoreRequest getMoreRequest,
      BongoSocket socket,
      WireProtocol wireProtocol,
      boolean requestCompression,
      BongoFindResponseConverter<TModel> findResponseConverterSkipBody,
      BobBsonConverter<BongoGetMoreRequest> getMoreRequestConverter,
      BlockingQueue<BobBsonBuffer> decodeQueue) {
    this.exhaustible = exhaustible;
    this.getMoreRequest = getMoreRequest;
    this.socket = socket;
    this.wireProtocol = wireProtocol;
    this.requestCompression = requestCompression;
    this.findResponseConverterSkipBody = findResponseConverterSkipBody;
    this.getMoreRequestConverter = getMoreRequestConverter;
    this.decodeQueue = decodeQueue;
  }

  @Override
  public Void call() throws Exception {
    DynamicBobBsonBuffer getMoreMessage = null;
    BongoFindResponse<TModel> result = null;
    do {
      if ((!exhaustible || result == null)) {
        log.debug("sending fetch more request");
        var lastGetMoreMessage = getMoreMessage;
        if (lastGetMoreMessage == null) {
          lastGetMoreMessage =
              wireProtocol.prepareCommandMessage(
                  socket,
                  getMoreRequestConverter,
                  getMoreRequest,
                  requestCompression,
                  exhaustible,
                  null,
                  socket.getNextRequestId());
        }

        for (var buf : lastGetMoreMessage.getBuffers()) {
          socket.write(buf);
        }

        getMoreMessage = lastGetMoreMessage;
      }

      WireProtocol.Response<BobBsonBuffer> getMoreResponse;
      log.debug("concurrent strategy reading new batch");
      getMoreResponse = wireProtocol.readRawServerResponse(socket, true);

      BsonReader reader = new BsonReader(getMoreResponse.getPayload());
      result = findResponseConverterSkipBody.read(reader);
      try {
        decodeQueue.put(getMoreResponse.getPayload());
      } catch (InterruptedException e) {
        log.debug(
            "Fetcher process interrupted waiting for space on queue for response" + " payload");
        break;
      }
    } while (result != null && result.getOk() == 1.0 && result.getId() != 0);

    if (getMoreMessage != null) {
      getMoreMessage.release();
    }

    return null;
  }
}
