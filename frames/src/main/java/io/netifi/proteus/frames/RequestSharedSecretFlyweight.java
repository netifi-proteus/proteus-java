package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class RequestSharedSecretFlyweight {
  private static final int PUBLIC_KEY_SIZE = 32;
  private static int TOKEN_SIZE = BitUtil.SIZE_OF_INT;

  private RequestSharedSecretFlyweight() {}

  public static int computeLength() {
    return FrameHeaderFlyweight.computeFrameHeaderLength() + TOKEN_SIZE + PUBLIC_KEY_SIZE;
  }

  public static int encode(ByteBuf byteBuf, int token, ByteBuf pk, long seqId) {
    int offset =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.REQUEST_SHARED_SECRET, 0, seqId);

    byteBuf.setInt(offset, token);
    offset += TOKEN_SIZE;

    byteBuf.setBytes(offset, pk);
    offset += PUBLIC_KEY_SIZE;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static int token(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    return byteBuf.getInt(offset);
  }

  public static ByteBuf publicKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + TOKEN_SIZE;
    return byteBuf.slice(offset, PUBLIC_KEY_SIZE);
  }
}
