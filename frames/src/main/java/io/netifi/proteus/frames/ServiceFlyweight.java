package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;

public class ServiceFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence fromDestination,
      CharSequence fromGroup,
      CharSequence service,
      ByteBuf metadata) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.SERVICE);

    int fromDestinationLength = ByteBufUtil.utf8Bytes(fromDestination);
    byteBuf.writeInt(fromDestinationLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, fromDestination, fromDestinationLength);

    int fromGroupLength = ByteBufUtil.utf8Bytes(fromGroup);
    byteBuf.writeInt(fromGroupLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, fromGroup, fromGroupLength);

    int serviceLength = ByteBufUtil.utf8Bytes(service);
    byteBuf.writeInt(serviceLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, service, serviceLength);

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

  public static String service(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int serviceLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, serviceLength, StandardCharsets.UTF_8);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int serviceLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + serviceLength;

    int metadataLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, metadataLength);
  }
}
