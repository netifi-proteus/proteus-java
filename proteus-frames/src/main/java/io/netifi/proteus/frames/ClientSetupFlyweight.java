package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.Objects;

public class ClientSetupFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator, long accessKey, byte[] accessToken, ByteBuf tags) {
    return encode(allocator, accessKey, Unpooled.wrappedBuffer(accessToken), tags);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator, long accessKey, ByteBuf accessToken, ByteBuf tags) {
    Objects.requireNonNull(tags);

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.CLIENT_SETUP);

    int accessTokenSize = accessToken.readableBytes();
    byteBuf
        .writeLong(accessKey)
        .writeInt(accessTokenSize)
        .writeBytes(accessToken, accessToken.readerIndex(), accessTokenSize)
        .writeBytes(tags, tags.readerIndex(), tags.readableBytes());

    return byteBuf;
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, accessTokenLength);
  }

  public static ByteBuf tags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + accessTokenLength;

    int tagsLength = byteBuf.readableBytes() - offset;
    return byteBuf.slice(offset, tagsLength);
  }
}
