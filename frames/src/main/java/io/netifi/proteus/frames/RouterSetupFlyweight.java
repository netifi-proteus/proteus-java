package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class RouterSetupFlyweight {
  private static final int CLUSTER_ID_SIZE = BitUtil.SIZE_OF_INT;
  private static final int ROUTER_ID_SIZE = BitUtil.SIZE_OF_INT;

  public static int computeLength(int tokenLength) {
    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + CLUSTER_ID_SIZE
        + ROUTER_ID_SIZE
        + tokenLength;
  }

  public static int encode(
      ByteBuf byteBuf, int clusterId, int routerId, ByteBuf authToken, long seqId) {
    int tokenLength = authToken.readableBytes();
    int length = computeLength(authToken.readableBytes());

    if (byteBuf.capacity() < length) {
      byteBuf.capacity(length);
    }

    int offset = FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.ROUTER_SETUP, 0, seqId);

    byteBuf.setInt(offset, clusterId);
    offset += CLUSTER_ID_SIZE;

    byteBuf.setInt(offset, routerId);
    offset += ROUTER_ID_SIZE;

    byteBuf.setBytes(offset, authToken);
    offset += tokenLength;

    byteBuf.writerIndex(offset);

    return length;
  }

  public static int clusterId(ByteBuf byteBuf) {
    return byteBuf.getInt(FrameHeaderFlyweight.computeFrameHeaderLength());
  }

  public static int routerId(ByteBuf byteBuf) {
    return byteBuf.getInt(FrameHeaderFlyweight.computeFrameHeaderLength() + CLUSTER_ID_SIZE);
  }

  public static ByteBuf authToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + ROUTER_ID_SIZE + CLUSTER_ID_SIZE;
    int tokenLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, tokenLength);
  }
}
