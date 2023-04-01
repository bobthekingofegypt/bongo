package org.bobstuff.bongo.monitoring;

import org.bobstuff.bobbson.BobBsonBuffer;
import org.bobstuff.bobbson.DynamicBobBsonBuffer;

public interface WireProtocolMonitor {
  void onReadServerResponse(BobBsonBuffer buffer);
  void onSendCommandMessage(int requestId, DynamicBobBsonBuffer buffer);
}
