package org.bobstuff.bongo.messages;

import org.bobstuff.bobbson.BobBsonBuffer;

public class BongoResponseHeaderParser {
  public static BongoResponseHeader read(BobBsonBuffer buffer, BongoResponseHeader responseHeader) {
    responseHeader.setMessageLength(buffer.getInt());
    responseHeader.setRequestId(buffer.getInt());
    responseHeader.setResponseTo(buffer.getInt());
    responseHeader.setOpCode(buffer.getInt());

    return responseHeader;
  }
}
