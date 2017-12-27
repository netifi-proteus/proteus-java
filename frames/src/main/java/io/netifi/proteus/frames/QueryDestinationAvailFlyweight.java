package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class QueryDestinationAvailFlyweight {
  private static final int ACCESS_KEY_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int ACCOUNT_ID_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int DESTINATION_ID_SIZE = BitUtil.SIZE_OF_LONG;
  private static int TOKEN_SIZE = BitUtil.SIZE_OF_INT;

  public static int computeLength(boolean hasToken) {
    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + (hasToken ? TOKEN_SIZE : 0)
        + ACCESS_KEY_SIZE
        + ACCOUNT_ID_SIZE
        + DESTINATION_ID_SIZE;
  }

  public static int encode(
      ByteBuf byteBuf,
      boolean hasToken,
      int token,
      long accessKey,
      long accountId,
      long destinationId,
      long seqId) {
    int flags = FrameHeaderFlyweight.encodeFlags(false, false, false, false, hasToken);
    int offset =
        FrameHeaderFlyweight.encodeFrameHeader(
            byteBuf, FrameType.QUERY_DESTINATION_AVAIL, flags, seqId);

    if (hasToken) {
      byteBuf.setInt(offset, token);
      offset += TOKEN_SIZE;
    }

    byteBuf.setLong(offset, accessKey);
    offset += ACCESS_KEY_SIZE;

    byteBuf.setLong(offset, accountId);
    offset += ACCOUNT_ID_SIZE;

    byteBuf.setLong(offset, destinationId);
    offset += DESTINATION_ID_SIZE;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static int token(ByteBuf byteBuf) {
    if (!FrameHeaderFlyweight.token(byteBuf)) {
      throw new IllegalStateException("no token present");
    }

    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    return byteBuf.getInt(offset);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0);
    return byteBuf.getLong(offset);
  }

  public static long accountId(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0)
            + ACCESS_KEY_SIZE;
    return byteBuf.getLong(offset);
  }

  public static long destinationId(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0)
            + ACCESS_KEY_SIZE
            + ACCOUNT_ID_SIZE;

    return byteBuf.getLong(offset);
  }
}

/*
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |0|Frame Type |U|M|E|B|A|T|   |    Major Ver    |     Min Ver   |
  +-----------------------------+-----------------+---------------+
  |                          HMAC Token                           |
  +---------------------------------------------------------------+
  |0|                                                             |
  +-+                        Access Key                           +
  |                                                               |
  +---------------------------------------------------------------+
  |0|                                                             |
  +-+                        Account Id                           +
  |                                                               |
  +---------------------------------------------------------------+
  |0|                                                             |
  +-+                     Destination Id                          +
  |                                                               |
  +---------------------------------------------------------------+
*/
