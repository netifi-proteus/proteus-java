package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class UnicastFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator, ByteBuf fromTags, ByteBuf toTags, ByteBuf metadata) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.UNICAST);

    int fromTagsLength = fromTags.readableBytes();
    byteBuf.writeInt(fromTagsLength);
    byteBuf.writeBytes(fromTags, fromTags.readerIndex(), fromTagsLength);

    int toTagsLength = toTags.readableBytes();
    byteBuf.writeInt(toTagsLength);
    byteBuf.writeBytes(toTags, toTags.readerIndex(), toTagsLength);

    byteBuf.writeBytes(metadata, metadata.readerIndex(), metadata.readableBytes());

    return byteBuf;
  }

  public static ByteBuf fromTags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromTagsLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, fromTagsLength);
  }

  public static ByteBuf toTags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromTagsLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromTagsLength;

    int toTagsLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, toTagsLength);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromTagsLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromTagsLength;

    int toTagsLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + toTagsLength;

    int metadataLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, metadataLength);
  }
}
