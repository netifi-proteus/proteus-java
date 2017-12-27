package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class QuerySetupFlyweight {

  private static final int ACCESS_TOKEN_SIZE = 20;
  private static final int ACCESS_KEY_SIZE = BitUtil.SIZE_OF_LONG;

  public static int computeLength() {
    return FrameHeaderFlyweight.computeFrameHeaderLength() + ACCESS_KEY_SIZE + ACCESS_TOKEN_SIZE;
  }

  public static int encode(ByteBuf byteBuf, ByteBuf accessToken, long accessKey, long seqId) {
    if (accessToken.capacity() != ACCESS_TOKEN_SIZE) {
      throw new IllegalStateException(
          String.format(
              "invalid access token size: found %d, expected %d",
              accessToken.capacity(), ACCESS_TOKEN_SIZE));
    }

    int offset = FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.QUERY_SETUP, 0, seqId);
    byteBuf.setBytes(offset, accessToken);
    offset += ACCESS_TOKEN_SIZE;

    byteBuf.setLong(offset, accessKey);
    offset += ACCESS_KEY_SIZE;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    return byteBuf.slice(FrameHeaderFlyweight.computeFrameHeaderLength(), ACCESS_TOKEN_SIZE);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + ACCESS_TOKEN_SIZE;
    return byteBuf.getLong(offset);
  }
}
