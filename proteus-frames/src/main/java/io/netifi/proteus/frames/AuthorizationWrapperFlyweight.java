package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class AuthorizationWrapperFlyweight {

  public static ByteBuf encode(ByteBufAllocator allocator, long accessKey, ByteBuf innerFrame) {

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.AUTHORIZATION_WRAPPER);

    byteBuf.writeLong(accessKey).writeBytes(innerFrame);

    return byteBuf;
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf innerFrame(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;
    offset += Long.BYTES;

    byteBuf.markReaderIndex();
    byteBuf.skipBytes(offset);
    ByteBuf slice = byteBuf.slice();
    byteBuf.resetReaderIndex();
    return slice;
  }
}
