package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class AuthenticationResponseFlyweight {
  private static final int ACCOUNT_ID_SIZE = BitUtil.SIZE_OF_INT;
  private static final int COUNT_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int SESSION_TOKEN = 20;
  private static final int SEQ_ID_SIZE = BitUtil.SIZE_OF_LONG;

  private AuthenticationResponseFlyweight() {}

  public static int computeLength() {
    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + ACCOUNT_ID_SIZE
        + SESSION_TOKEN
        + COUNT_SIZE
        + SEQ_ID_SIZE;
  }

  public static int encode(
      ByteBuf byteBuf, long accountId, long count, byte[] sessionToken, long seqId) {
    int offset = FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.AUTH_RESPONSE, 0, seqId);

    byteBuf.setLong(offset, accountId);
    offset += ACCOUNT_ID_SIZE;

    byteBuf.setLong(offset, count);
    offset += COUNT_SIZE;

    byteBuf.setBytes(offset, sessionToken);
    offset += SESSION_TOKEN;

    byteBuf.setLong(offset, seqId);
    offset += offset;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static long accountId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    return byteBuf.getLong(offset);
  }

  public static long count(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    offset += ACCOUNT_ID_SIZE;
    return byteBuf.getLong(offset);
  }

  public static byte[] sessionToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    offset += ACCOUNT_ID_SIZE + COUNT_SIZE;

    byte[] bytes = new byte[20];
    byteBuf.getBytes(offset, bytes);

    return bytes;
  }

  public static long seqId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    offset += ACCOUNT_ID_SIZE + SESSION_TOKEN + COUNT_SIZE;

    return byteBuf.getLong(offset);
  }
}
