package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DestinationSetupFlyweight {

  private static final int ACCESS_KEY_LENGTH_SIZE = Long.BYTES;

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence destination,
      CharSequence group,
      long accessKey,
      byte[] accessToken) {
    return encode(allocator, destination, group, accessKey, Unpooled.wrappedBuffer(accessToken));
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence destination,
      CharSequence group,
      long accessKey,
      ByteBuf accessToken) {
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);

    ByteBuf buffer = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.DESTINATION_SETUP);

    ByteBuf destinationBuffer = allocator.buffer();
    int destinationLength =
        destinationBuffer.writeCharSequence(destination, StandardCharsets.UTF_8);

    ByteBuf groupBuffer = allocator.buffer();
    int groupLength = groupBuffer.writeCharSequence(group, StandardCharsets.UTF_8);

    int accessTokenSize = accessToken.readableBytes();

    ByteBuf byteBuf =
        buffer
            .writeInt(destinationLength)
            .writeBytes(destinationBuffer)
            .writeInt(groupLength)
            .writeBytes(groupBuffer)
            .writeLong(accessKey)
            .writeInt(accessTokenSize)
            .writeBytes(accessToken);

    ReferenceCountUtil.safeRelease(destinationBuffer);
    ReferenceCountUtil.safeRelease(groupBuffer);

    return byteBuf;
  }

  public static CharSequence destination(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();

    int offset = FrameHeaderFlyweight.size(byteBuf);
    byteBuf.readerIndex(offset);

    int length = byteBuf.readInt();

    return byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
  }

  public static CharSequence group(ByteBuf byteBuf) {

    byteBuf.resetReaderIndex();

    int offset = FrameHeaderFlyweight.size(byteBuf);
    byteBuf.readerIndex(offset);

    offset = byteBuf.readInt();
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);

    int length = byteBuf.readInt();

    return byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
  }

  public static long accessKey(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();

    int offset = FrameHeaderFlyweight.size(byteBuf);
    byteBuf.readerIndex(offset);

    offset = byteBuf.readInt();
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);

    offset = byteBuf.readInt();
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);

    return byteBuf.readLong();
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();

    int offset = FrameHeaderFlyweight.size(byteBuf);
    byteBuf.readerIndex(offset);

    offset = byteBuf.readInt();
    byteBuf.readerIndex(byteBuf.readerIndex() + offset);

    offset = byteBuf.readInt();
    byteBuf.readerIndex(byteBuf.readerIndex() + offset + ACCESS_KEY_LENGTH_SIZE);

    byteBuf.readInt();

    return byteBuf.slice();
  }
}
