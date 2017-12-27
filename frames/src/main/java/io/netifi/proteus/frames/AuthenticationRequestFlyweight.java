package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class AuthenticationRequestFlyweight {
  private static final int ACCESS_TOKEN_SIZE = 20;
  private static final int ACCESS_KEY_SIZE = BitUtil.SIZE_OF_LONG;

  private AuthenticationRequestFlyweight() {}

  public static int computeLength() {
    return FrameHeaderFlyweight.computeFrameHeaderLength() + ACCESS_KEY_SIZE + ACCESS_TOKEN_SIZE;
  }

  public static int encode(ByteBuf byteBuf, ByteBuf accessToken, long accessKey, long seqId) {
    int offset = FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.AUTH_REQUEST, 0, seqId);
    byte[] at = new byte[ACCESS_TOKEN_SIZE];
    accessToken.getBytes(0, at);
    byteBuf.setBytes(offset, at);
    offset += ACCESS_TOKEN_SIZE;
    byteBuf.setLong(offset, accessKey);
    offset += ACCESS_KEY_SIZE;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    return byteBuf.slice(offset, ACCESS_TOKEN_SIZE);
  }

  public static byte[] accessTokenByteArray(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    byte[] accessToken = new byte[ACCESS_TOKEN_SIZE];
    byteBuf.getBytes(offset, accessToken);
    return accessToken;
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + ACCESS_TOKEN_SIZE;
    return byteBuf.getLong(offset);
  }
}
