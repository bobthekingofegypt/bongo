package org.bobstuff.bongo.executionstrategy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.BobBsonConverter;
import org.bobstuff.bobbson.DynamicBobBsonBuffer;
import org.bobstuff.bobbson.buffer.BobBufferPool;
import org.bobstuff.bobbson.writer.BsonWriter;
import org.bobstuff.bongo.BongoCollection;
import org.bobstuff.bongo.BongoCursorType;
import org.bobstuff.bongo.WireProtocol;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bobstuff.bongo.connection.BongoSocket;
import org.bobstuff.bongo.exception.BongoException;
import org.bobstuff.bongo.exception.BongoReadException;
import org.bobstuff.bongo.messages.BongoFindRequest;
import org.bobstuff.bongo.messages.BongoFindResponse;
import org.bobstuff.bongo.messages.BongoGetMoreRequest;
import org.bobstuff.bongo.messages.BongoResponseHeader;
import org.bobstuff.bongo.topology.BongoConnectionProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class ReadExecutionConcurrentStrategyTest {
  static class Model {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testErrorFromParserDestroySocketExhaustible() {
    var strategy = new ReadExecutionConcurrentStrategy<Model>(1, 1);
    var identifier = new BongoCollection.Identifier("testdatabase", "testcollection");
    var wireProtocol = Mockito.mock(WireProtocol.class, Mockito.withSettings().verboseLogging());
    var codec = Mockito.mock(BongoCodec.class);
    var connectionProvider = Mockito.mock(BongoConnectionProvider.class);
    var bufferPool = new BobBufferPool();
    var socket = Mockito.mock(BongoSocket.class);
    var modelOne = new Model();

    var header = new BongoResponseHeader();
    var initialFindResponse = new BongoFindResponse<Model>();
    initialFindResponse.setOk(1.0);
    initialFindResponse.setId(1234);
    initialFindResponse.setBatch(List.of(modelOne));
    var initialResponse = new WireProtocol.Response<>(header, 0, initialFindResponse);

    var getMoreRequest = new DynamicBobBsonBuffer(bufferPool);
    getMoreRequest.writeBytes(new byte[] {1, 2, 3, 4, 5});

    var getMoreRequestConverter = Mockito.mock(BobBsonConverter.class);
    when(codec.converter(BongoGetMoreRequest.class)).thenReturn(getMoreRequestConverter);

    var buffer = new DynamicBobBsonBuffer(new BobBufferPool());
    buffer.setTail(5);
    var bsonWriter = new BsonWriter(buffer);
    bsonWriter.writeStartDocument();
    bsonWriter.writeStartDocument("cursor");
    bsonWriter.writeLong("id", 1234L);
    bsonWriter.writeStartArray("nextBatch");
    bsonWriter.writeString("this will break response parser");
    bsonWriter.writeEndArray();
    bsonWriter.writeEndDocument();
    bsonWriter.writeDouble("ok", 1.0);
    bsonWriter.writeEndDocument();

    var getMoreResponseHeader = new BongoResponseHeader();
    var getMoreResponse = new WireProtocol.Response<BobBsonBuffer>(header, 0, buffer);

    when(connectionProvider.getReadConnection()).thenReturn(socket);
    when(wireProtocol.sendReceiveCommandMessage(
            eq(socket),
            any(BobBsonConverter.class),
            any(),
            any(BobBsonConverter.class),
            eq(false),
            eq(false)))
        .thenReturn(initialResponse);
    when(wireProtocol.prepareCommandMessage(
            eq(socket),
            eq(getMoreRequestConverter),
            any(),
            eq(false),
            eq(true),
            Mockito.isNull(),
            any(Integer.class)))
        .thenReturn(getMoreRequest);
    when(wireProtocol.readRawServerResponse(any(), eq(true))).thenReturn(getMoreResponse);

    var requestConverter =
        (BobBsonConverter<BongoFindRequest>) Mockito.mock(BobBsonConverter.class);
    var bongoRequest = Mockito.mock(BongoFindRequest.class);

    var cursor =
        strategy.execute(
            identifier,
            requestConverter,
            bongoRequest,
            Model.class,
            10,
            false,
            BongoCursorType.Exhaustible,
            wireProtocol,
            codec,
            bufferPool,
            connectionProvider);

    Assertions.assertTrue(cursor.hasNext());
    var firstList = cursor.next();
    Assertions.assertThrows(BongoException.class, cursor::hasNext);

    cursor.close();
    strategy.close();

    Assertions.assertTrue(strategy.isClosed());
    Mockito.verify(socket).close();
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testNoMoreCompletes() {
    var strategy = new ReadExecutionConcurrentStrategy<Model>(1, 1);
    var identifier = new BongoCollection.Identifier("testdatabase", "testcollection");
    var wireProtocol = Mockito.mock(WireProtocol.class, Mockito.withSettings().verboseLogging());
    var codec = Mockito.mock(BongoCodec.class);
    var connectionProvider = Mockito.mock(BongoConnectionProvider.class);
    var bufferPool = new BobBufferPool();
    var socket = Mockito.mock(BongoSocket.class);
    var modelOne = new Model();

    var header = new BongoResponseHeader();
    var initialFindResponse = new BongoFindResponse<Model>();
    initialFindResponse.setOk(1.0);
    initialFindResponse.setId(0);
    initialFindResponse.setBatch(List.of(modelOne));
    var initialResponse = new WireProtocol.Response<>(header, 0, initialFindResponse);

    when(connectionProvider.getReadConnection()).thenReturn(socket);
    when(wireProtocol.sendReceiveCommandMessage(
            eq(socket),
            any(BobBsonConverter.class),
            any(),
            any(BobBsonConverter.class),
            eq(false),
            eq(false)))
        .thenReturn(initialResponse);

    var requestConverter =
        (BobBsonConverter<BongoFindRequest>) Mockito.mock(BobBsonConverter.class);
    var bongoRequest = Mockito.mock(BongoFindRequest.class);

    var cursor =
        strategy.execute(
            identifier,
            requestConverter,
            bongoRequest,
            Model.class,
            10,
            false,
            BongoCursorType.Exhaustible,
            wireProtocol,
            codec,
            bufferPool,
            connectionProvider);

    Assertions.assertNotNull(cursor);
    cursor.next();
    Assertions.assertFalse(cursor.hasNext());
    Mockito.verify(socket).release();
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testNotOkFromFirstRequest() {
    var strategy = new ReadExecutionConcurrentStrategy<Model>(1, 1);
    var identifier = new BongoCollection.Identifier("testdatabase", "testcollection");
    var wireProtocol = Mockito.mock(WireProtocol.class, Mockito.withSettings().verboseLogging());
    var codec = Mockito.mock(BongoCodec.class);
    var connectionProvider = Mockito.mock(BongoConnectionProvider.class);
    var bufferPool = new BobBufferPool();
    var socket = Mockito.mock(BongoSocket.class);
    var modelOne = new Model();

    var header = new BongoResponseHeader();
    var initialFindResponse = new BongoFindResponse<Model>();
    initialFindResponse.setOk(0.0);
    initialFindResponse.setId(1234);
    initialFindResponse.setBatch(List.of(modelOne));
    var initialResponse = new WireProtocol.Response<>(header, 0, initialFindResponse);

    when(connectionProvider.getReadConnection()).thenReturn(socket);
    when(wireProtocol.sendReceiveCommandMessage(
            eq(socket),
            any(BobBsonConverter.class),
            any(),
            any(BobBsonConverter.class),
            eq(false),
            eq(false)))
        .thenReturn(initialResponse);

    Assertions.assertThrows(
        BongoReadException.class,
        () -> {
          var requestConverter =
              (BobBsonConverter<BongoFindRequest>) Mockito.mock(BobBsonConverter.class);
          var bongoRequest = Mockito.mock(BongoFindRequest.class);

          strategy.execute(
              identifier,
              requestConverter,
              bongoRequest,
              Model.class,
              10,
              false,
              BongoCursorType.Exhaustible,
              wireProtocol,
              codec,
              bufferPool,
              connectionProvider);
        });

    Mockito.verify(socket).release();
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testErrorFromFirstRequest() {
    var strategy = new ReadExecutionConcurrentStrategy<Model>(1, 1);
    var identifier = new BongoCollection.Identifier("testdatabase", "testcollection");
    var wireProtocol = Mockito.mock(WireProtocol.class, Mockito.withSettings().verboseLogging());
    var codec = Mockito.mock(BongoCodec.class);
    var connectionProvider = Mockito.mock(BongoConnectionProvider.class);
    var bufferPool = new BobBufferPool();
    var socket = Mockito.mock(BongoSocket.class);

    when(connectionProvider.getReadConnection()).thenReturn(socket);
    when(wireProtocol.sendReceiveCommandMessage(
            eq(socket),
            any(BobBsonConverter.class),
            any(),
            any(BobBsonConverter.class),
            eq(false),
            eq(false)))
        .thenThrow(new BongoException("Unit test exception sending initial request"));

    Assertions.assertThrows(
        BongoException.class,
        () -> {
          var requestConverter =
              (BobBsonConverter<BongoFindRequest>) Mockito.mock(BobBsonConverter.class);
          var bongoRequest = Mockito.mock(BongoFindRequest.class);

          strategy.execute(
              identifier,
              requestConverter,
              bongoRequest,
              Model.class,
              10,
              false,
              BongoCursorType.Exhaustible,
              wireProtocol,
              codec,
              bufferPool,
              connectionProvider);
        });
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testErrorFromParser() {
    var strategy = new ReadExecutionConcurrentStrategy<Model>(1, 1);
    var identifier = new BongoCollection.Identifier("testdatabase", "testcollection");
    var wireProtocol = Mockito.mock(WireProtocol.class, Mockito.withSettings().verboseLogging());
    var codec = Mockito.mock(BongoCodec.class);
    var connectionProvider = Mockito.mock(BongoConnectionProvider.class);
    var bufferPool = new BobBufferPool();
    var socket = Mockito.mock(BongoSocket.class);
    var modelOne = new Model();

    var header = new BongoResponseHeader();
    var initialFindResponse = new BongoFindResponse<Model>();
    initialFindResponse.setOk(1.0);
    initialFindResponse.setId(1234);
    initialFindResponse.setBatch(List.of(modelOne));
    var initialResponse = new WireProtocol.Response<>(header, 0, initialFindResponse);

    var getMoreRequest = new DynamicBobBsonBuffer(bufferPool);
    getMoreRequest.writeBytes(new byte[] {1, 2, 3, 4, 5});

    var getMoreRequestConverter = Mockito.mock(BobBsonConverter.class);
    when(codec.converter(BongoGetMoreRequest.class)).thenReturn(getMoreRequestConverter);

    var buffer = new DynamicBobBsonBuffer(new BobBufferPool());
    buffer.setTail(5);
    var bsonWriter = new BsonWriter(buffer);
    bsonWriter.writeStartDocument();
    bsonWriter.writeStartDocument("cursor");
    bsonWriter.writeLong("id", 1234L);
    bsonWriter.writeStartArray("nextBatch");
    bsonWriter.writeString("this will break response parser");
    bsonWriter.writeEndArray();
    bsonWriter.writeEndDocument();
    bsonWriter.writeDouble("ok", 1.0);
    bsonWriter.writeEndDocument();

    var getMoreResponseHeader = new BongoResponseHeader();
    var getMoreResponse = new WireProtocol.Response<BobBsonBuffer>(header, 0, buffer);

    when(connectionProvider.getReadConnection()).thenReturn(socket);
    when(wireProtocol.sendReceiveCommandMessage(
            eq(socket),
            any(BobBsonConverter.class),
            any(),
            any(BobBsonConverter.class),
            eq(false),
            eq(false)))
        .thenReturn(initialResponse);
    when(wireProtocol.prepareCommandMessage(
            eq(socket),
            eq(getMoreRequestConverter),
            any(),
            eq(false),
            eq(true),
            Mockito.isNull(),
            any(Integer.class)))
        .thenReturn(getMoreRequest);
    when(wireProtocol.readRawServerResponse(any(), eq(true))).thenReturn(getMoreResponse);

    var requestConverter =
        (BobBsonConverter<BongoFindRequest>) Mockito.mock(BobBsonConverter.class);
    var bongoRequest = Mockito.mock(BongoFindRequest.class);

    var cursor =
        strategy.execute(
            identifier,
            requestConverter,
            bongoRequest,
            Model.class,
            10,
            false,
            BongoCursorType.Exhaustible,
            wireProtocol,
            codec,
            bufferPool,
            connectionProvider);

    Assertions.assertTrue(cursor.hasNext());
    var firstList = cursor.next();
    Assertions.assertThrows(BongoException.class, cursor::hasNext);

    cursor.close();
    strategy.close();

    Assertions.assertTrue(strategy.isClosed());
    Mockito.verify(socket).close();
  }

  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testClose() {
    var strategy = new ReadExecutionConcurrentStrategy<Model>(1, 1);
    var identifier = new BongoCollection.Identifier("testdatabase", "testcollection");
    var wireProtocol = Mockito.mock(WireProtocol.class, Mockito.withSettings().verboseLogging());
    var codec = Mockito.mock(BongoCodec.class);
    var connectionProvider = Mockito.mock(BongoConnectionProvider.class);
    var bufferPool = new BobBufferPool();
    var socket = Mockito.mock(BongoSocket.class);
    var modelOne = new Model();

    var header = new BongoResponseHeader();
    var initialFindResponse = new BongoFindResponse<Model>();
    initialFindResponse.setOk(1.0);
    initialFindResponse.setId(1234);
    initialFindResponse.setBatch(List.of(modelOne));
    var initialResponse = new WireProtocol.Response<>(header, 0, initialFindResponse);

    var getMoreRequest = new DynamicBobBsonBuffer(bufferPool);
    getMoreRequest.writeBytes(new byte[] {1, 2, 3, 4, 5});

    var getMoreRequestConverter = Mockito.mock(BobBsonConverter.class);
    when(codec.converter(BongoGetMoreRequest.class)).thenReturn(getMoreRequestConverter);

    //    var getMoreResponseHeader = new BongoResponseHeader();
    //    var getMoreResponse = new BongoFindResponse<Model>();
    //    initialFindResponse.setOk(1.0);
    //    initialFindResponse.setId(1234);
    //    initialFindResponse.setBatch(List.of(modelOne));
    //    var initialResponse = new WireProtocol.Response<>(header, 0, initialFindResponse);

    when(connectionProvider.getReadConnection()).thenReturn(socket);
    when(wireProtocol.sendReceiveCommandMessage(
            eq(socket),
            any(BobBsonConverter.class),
            any(),
            any(BobBsonConverter.class),
            eq(false),
            eq(false)))
        .thenReturn(initialResponse);
    when(wireProtocol.prepareCommandMessage(
            eq(socket),
            eq(getMoreRequestConverter),
            any(),
            eq(false),
            eq(true),
            Mockito.isNull(),
            any(Integer.class)))
        .thenReturn(getMoreRequest);
    when(wireProtocol.readRawServerResponse(any(), eq(true)))
        .thenThrow(new BongoException("failing in unit test"));

    var requestConverter =
        (BobBsonConverter<BongoFindRequest>) Mockito.mock(BobBsonConverter.class);
    var bongoRequest = Mockito.mock(BongoFindRequest.class);

    var cursor =
        strategy.execute(
            identifier,
            requestConverter,
            bongoRequest,
            Model.class,
            10,
            false,
            BongoCursorType.Exhaustible,
            wireProtocol,
            codec,
            bufferPool,
            connectionProvider);

    Assertions.assertTrue(cursor.hasNext());
    var firstList = cursor.next();
    Assertions.assertThrows(BongoException.class, cursor::hasNext);

    cursor.close();
    strategy.close();

    Assertions.assertTrue(strategy.isClosed());
  }
}
