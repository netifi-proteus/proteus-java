package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

public class ShardFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence fromDestination,
      CharSequence fromGroup,
      CharSequence toGroup,
      ByteBuf shardKey,
      ByteBuf metadata) {

    ByteBuf fromDestinationBuffer = allocator.buffer();
    int fromDestinationLength =
        fromDestinationBuffer.writeCharSequence(fromDestination, StandardCharsets.UTF_8);

    ByteBuf fromGroupBuffer = allocator.buffer();
    int fromGroupLength = fromGroupBuffer.writeCharSequence(fromGroup, StandardCharsets.UTF_8);

    ByteBuf toGroupBuffer = allocator.buffer();
    int toGroupLength = toGroupBuffer.writeCharSequence(toGroup, StandardCharsets.UTF_8);

    int shardKeyLength = shardKey.readableBytes();

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.DESTINATION)
            .writeInt(fromDestinationLength)
            .writeBytes(fromDestinationBuffer)
            .writeInt(fromGroupLength)
            .writeBytes(fromGroupBuffer)
            .writeInt(toGroupLength)
            .writeBytes(toGroupBuffer)
            .writeInt(shardKeyLength)
            .writeBytes(shardKey);

    return Unpooled.wrappedBuffer(byteBuf, metadata);
  }

  public static CharSequence fromDestination(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    int offset = FrameHeaderFlyweight.size(byteBuf);
    byteBuf.readerIndex(offset);
    int length = byteBuf.readInt();

    return byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
  }

  public static CharSequence fromGroup(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    int offset = FrameHeaderFlyweight.size(byteBuf);

    byteBuf.readerIndex(offset);
    offset = byteBuf.readInt();

    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    int length = byteBuf.readInt();

    return byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
  }

  public static CharSequence toGroup(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    int offset = FrameHeaderFlyweight.size(byteBuf);

    byteBuf.readerIndex(offset);
    offset = byteBuf.readInt();

    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    offset = byteBuf.readInt();

    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    int length = byteBuf.readInt();

    return byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
  }

  public static ByteBuf shardKey(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    int offset = FrameHeaderFlyweight.size(byteBuf);
  
    byteBuf.readerIndex(offset);
    offset = byteBuf.readInt();
  
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    offset = byteBuf.readInt();
  
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    offset = byteBuf.readInt();
  
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    int length = byteBuf.readInt();
    
    return byteBuf.slice(byteBuf.readerIndex(), length);
  }
  
  public static ByteBuf metadata(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();
    int offset = FrameHeaderFlyweight.size(byteBuf);
  
    byteBuf.readerIndex(offset);
    offset = byteBuf.readInt();
  
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    offset = byteBuf.readInt();
  
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    offset = byteBuf.readInt();
  
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    offset = byteBuf.readInt();
    
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);
    return byteBuf.slice();
  }
}
