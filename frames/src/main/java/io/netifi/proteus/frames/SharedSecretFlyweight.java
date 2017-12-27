package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class SharedSecretFlyweight {
  private static final int PUBLIC_KEY_SIZE = 32;
  private static final int SHARED_SECRET_SIZE = 16;
  private static int TOKEN_SIZE = BitUtil.SIZE_OF_INT;

  private SharedSecretFlyweight() {}

  public static int computeLength() {
    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + TOKEN_SIZE
        + PUBLIC_KEY_SIZE
        + SHARED_SECRET_SIZE;
  }

  public static int encode(
      ByteBuf byteBuf, int token, ByteBuf publicKey, ByteBuf sharedSecret, long seqId) {
    int offset = FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.SHARED_SECRET, 0, seqId);

    byteBuf.setInt(offset, token);
    offset += TOKEN_SIZE;

    byteBuf.setBytes(offset, publicKey);
    offset += PUBLIC_KEY_SIZE;

    byteBuf.setBytes(offset, sharedSecret);
    offset += SHARED_SECRET_SIZE;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static ByteBuf publicKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + TOKEN_SIZE;
    return byteBuf.slice(offset, PUBLIC_KEY_SIZE);
  }

  public static int token(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    return byteBuf.getInt(offset);
  }

  public static ByteBuf sharedSecret(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + TOKEN_SIZE + PUBLIC_KEY_SIZE;
    return byteBuf.slice(offset, SHARED_SECRET_SIZE);
  }
}
