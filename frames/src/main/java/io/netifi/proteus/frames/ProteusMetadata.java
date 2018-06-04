package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class ProteusMetadata {
  private static final int VERSION_SIZE = Short.BYTES;
  private static final int NAMESPACE_ID_SIZE = Integer.BYTES;
  private static final int SERVICE_ID_SIZE = Integer.BYTES;
  private static final int METHOD_ID_SIZE = Integer.BYTES;

  public static ByteBuf encode(
      ByteBufAllocator allocator, int namespaceId, int serviceId, int methodId, ByteBuf metadata) {
    ByteBuf byteBuf = allocator.buffer()
        .writeShort(1)
        .writeInt(namespaceId)
        .writeInt(serviceId)
        .writeInt(methodId)
        .writeBytes(metadata, metadata.readerIndex(), metadata.readableBytes());

    return byteBuf;
  }

  public static int version(ByteBuf byteBuf) {
    return byteBuf.getShort(0) & 0x7FFF;
  }

  public static int namespaceId(ByteBuf byteBuf) {
    int offset = VERSION_SIZE;
    return byteBuf.getInt(offset);
  }

  public static int serviceId(ByteBuf byteBuf) {
    int offset = VERSION_SIZE + NAMESPACE_ID_SIZE;
    return byteBuf.getInt(offset);
  }

  public static int methodId(ByteBuf byteBuf) {
    int offset = VERSION_SIZE + NAMESPACE_ID_SIZE + SERVICE_ID_SIZE;
    return byteBuf.getInt(offset);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    int offset = VERSION_SIZE + NAMESPACE_ID_SIZE + SERVICE_ID_SIZE + METHOD_ID_SIZE;
    int metadataLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, metadataLength);
  }
}
