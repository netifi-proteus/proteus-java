package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class DestinationSetupFlyweight {
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

    ByteBuf destinationBuffer = ByteBufUtil.writeUtf8(allocator, destination);
    ByteBuf groupBuffer = ByteBufUtil.writeUtf8(allocator, group);

    int accessTokenSize = accessToken.readableBytes();

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.DESTINATION_SETUP)
            .writeInt(destinationBuffer.readableBytes())
            .writeBytes(destinationBuffer)
            .writeInt(groupBuffer.readableBytes())
            .writeBytes(groupBuffer)
            .writeLong(accessKey)
            .writeInt(accessTokenSize)
            .writeBytes(accessToken, accessToken.readerIndex(), accessTokenSize);

    ReferenceCountUtil.safeRelease(destinationBuffer);
    ReferenceCountUtil.safeRelease(groupBuffer);

    return byteBuf;
  }

  public static CharSequence destination(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int destinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, destinationLength, StandardCharsets.UTF_8);
  }

  public static CharSequence group(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int destinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + destinationLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, groupLength, StandardCharsets.UTF_8);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int destinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + destinationLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int destinationLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + destinationLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, accessTokenLength);
  }
}
