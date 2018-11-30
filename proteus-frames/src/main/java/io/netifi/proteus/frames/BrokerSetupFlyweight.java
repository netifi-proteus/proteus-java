package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class BrokerSetupFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      int clusterId,
      int brokerId,
      long accessKey,
      ByteBuf accessToken) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.BROKER_SETUP);

    byteBuf.writeInt(clusterId);
    byteBuf.writeInt(brokerId);

    int authTokenSize = accessToken.readableBytes();
    byteBuf
        .writeLong(accessKey)
        .writeInt(authTokenSize)
        .writeBytes(accessToken, accessToken.readerIndex(), authTokenSize);

    return byteBuf;
  }

  public static int clusterId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    return byteBuf.getInt(offset);
  }

  public static int brokerId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES + Integer.BYTES;

    return byteBuf.getInt(offset);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES + Integer.BYTES + Integer.BYTES;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, accessTokenLength);
  }
}
