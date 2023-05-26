package org.bobstuff.bongo.monitoring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.bobstuff.bobbson.BsonReader;
import org.bobstuff.bobbson.BsonReaderStack;
import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.buffer.DynamicBobBsonBuffer;
import org.bobstuff.bongo.codec.BongoCodec;
import org.bson.BsonDocument;

// TODO move this out of the project, stick it in examples
public class WireProtocolMonitorFileWrite implements WireProtocolMonitor {
  private BongoCodec codec;
  public WireProtocolMonitorFileWrite(BongoCodec codec) {
    this.codec = codec;
  }
  @Override
  public void onReadServerResponse(BobBsonBuffer buffer) {
        buffer.setHead(5);
        var readerDebug = new BsonReaderStack(buffer);
        var responseDebug = codec.decode(BsonDocument.class, readerDebug);
        if (responseDebug != null) {
          System.out.println("*********************");
          System.out.println(responseDebug);
        }
  }

  @Override
  public void onSendCommandMessage(int requestId, DynamicBobBsonBuffer buffer) {
    var bos = new ByteArrayOutputStream();
    for (var buf : buffer.getBuffers()) {
      bos.write(buf.getArray(), buf.getHead(), buf.getTail());
    }
    var tmpdir = System.getProperty("java.io.tmpdir");
    try {
      Files.write(Path.of(tmpdir, "sendCommandMessage-" + requestId), bos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
