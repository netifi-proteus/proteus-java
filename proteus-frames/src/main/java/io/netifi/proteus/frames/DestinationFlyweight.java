package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.StandardCharsets;

public class DestinationFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence fromDestination,
      CharSequence fromGroup,
      CharSequence toDestination,
      CharSequence toGroup,
      ByteBuf metadata) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.DESTINATION);

    int fromDestinationLength = ByteBufUtil.utf8Bytes(fromDestination);
    byteBuf.writeInt(fromDestinationLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, fromDestination, fromDestinationLength);

    int fromGroupLength = ByteBufUtil.utf8Bytes(fromGroup);
    byteBuf.writeInt(fromGroupLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, fromGroup, fromGroupLength);

    int toDestinationLength = ByteBufUtil.utf8Bytes(toDestination);
    byteBuf.writeInt(toDestinationLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, toDestination, toDestinationLength);

    int toGroupLength = ByteBufUtil.utf8Bytes(toGroup);
    byteBuf.writeInt(toGroupLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, toGroup, toGroupLength);

    byteBuf.writeBytes(metadata, metadata.readerIndex(), metadata.readableBytes());

    return byteBuf;
  }

  public static String fromDestination(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, fromDestinationLength, StandardCharsets.UTF_8);
  }

  public static String fromGroup(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, fromGroupLength, StandardCharsets.UTF_8);
  }

  public static String toDestination(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int toDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, toDestinationLength, StandardCharsets.UTF_8);
  }

  public static String toGroup(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int toDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + toDestinationLength;

    int toGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, toGroupLength, StandardCharsets.UTF_8);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int toDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + toDestinationLength;

    int toGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + toGroupLength;

    int metadataLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, metadataLength);
  }
}
