package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;

public class ShardFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence fromDestination,
      CharSequence fromGroup,
      CharSequence toGroup,
      ByteBuf shardKey,
      ByteBuf metadata) {

    ByteBuf fromDestinationBuffer = ByteBufUtil.writeUtf8(allocator, fromDestination);
    ByteBuf fromGroupBuffer = ByteBufUtil.writeUtf8(allocator, fromGroup);
    ByteBuf toGroupBuffer = ByteBufUtil.writeUtf8(allocator, toGroup);

    int shardKeyLength = shardKey.readableBytes();

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.DESTINATION)
            .writeInt(fromDestinationBuffer.readableBytes())
            .writeBytes(fromDestinationBuffer)
            .writeInt(fromGroupBuffer.readableBytes())
            .writeBytes(fromGroupBuffer)
            .writeInt(toGroupBuffer.readableBytes())
            .writeBytes(toGroupBuffer)
            .writeInt(shardKeyLength)
            .writeBytes(shardKey, shardKey.readerIndex(), shardKeyLength)
            .writeBytes(metadata, metadata.readerIndex(), metadata.readableBytes());

    ReferenceCountUtil.safeRelease(fromDestinationBuffer);
    ReferenceCountUtil.safeRelease(fromGroupBuffer);
    ReferenceCountUtil.safeRelease(toGroupBuffer);

    return byteBuf;
  }

  public static CharSequence fromDestination(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, fromDestinationLength, StandardCharsets.UTF_8);
  }

  public static CharSequence fromGroup(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, fromGroupLength, StandardCharsets.UTF_8);
  }

  public static CharSequence toGroup(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int toGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, toGroupLength, StandardCharsets.UTF_8);
  }

  public static ByteBuf shardKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int toGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + toGroupLength;

    int shardKeyLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, shardKeyLength);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int fromDestinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromDestinationLength;

    int fromGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + fromGroupLength;

    int toGroupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + toGroupLength;

    int shardKeyLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + shardKeyLength;

    int metadataLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, metadataLength);
  }
}
