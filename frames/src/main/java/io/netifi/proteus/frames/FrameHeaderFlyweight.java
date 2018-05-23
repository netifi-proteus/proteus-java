package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class FrameHeaderFlyweight {

  // Protocol Version
  public static final short MAJOR_VERSION = 0;
  public static final short MINOR_VERSION = 1;

  private static final int MAJOR_VERSION_SIZE = Short.BYTES;
  private static final int MINOR_VERSION_SIZE = Short.BYTES;

  private FrameHeaderFlyweight() {}

  public static ByteBuf encodeFrameHeader(
      final ByteBufAllocator allocator,
      final short majorVersion,
      final short minorVersion,
      final FrameType type) {
    return allocator
        .buffer()
        .writeShort(majorVersion)
        .writeShort(minorVersion)
        .writeShort(type.getEncodedType());
  }

  public static ByteBuf encodeFrameHeader(final ByteBufAllocator allocator, final FrameType type) {
    return encodeFrameHeader(allocator, MAJOR_VERSION, MINOR_VERSION, type);
  }

  public static short majorVersion(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    return byteBuf.readShort();
  }

  public static short minorVersion(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    byteBuf.readerIndex(MAJOR_VERSION_SIZE);
    return byteBuf.readShort();
  }

  public static FrameType frameType(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    byteBuf.readerIndex(MAJOR_VERSION_SIZE + MINOR_VERSION_SIZE);
    short frameTypeId = byteBuf.readShort();
    return FrameType.from(frameTypeId);
  }

  public static int size(ByteBuf byteBuf) {
    return Short.BYTES * 3;
  }
}
