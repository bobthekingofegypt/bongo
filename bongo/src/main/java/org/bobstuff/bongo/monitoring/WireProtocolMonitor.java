package org.bobstuff.bongo.monitoring;

import org.bobstuff.bobbson.buffer.BobBsonBuffer;
import org.bobstuff.bobbson.buffer.DynamicBobBsonBuffer;

public interface WireProtocolMonitor {
  void onReadServerResponse(BobBsonBuffer buffer);
  void onSendCommandMessage(int requestId, DynamicBobBsonBuffer buffer);
}
