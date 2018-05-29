package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class BrokerSetupFlyweight {
  private static final int ACCESS_KEY_LENGTH_SIZE = Long.BYTES;

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence brokerId,
      CharSequence clusterId,
      long accessKey,
      ByteBuf accessToken) {

    Objects.requireNonNull(brokerId);
    Objects.requireNonNull(clusterId);

    ByteBuf buffer = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.BROKER_SETUP);

    ByteBuf brokerIdBuffer = allocator.buffer();
    int routerIdLength = brokerIdBuffer.writeCharSequence(brokerId, StandardCharsets.UTF_8);

    ByteBuf clusterIdBuffer = allocator.buffer();
    int clusterIdLength = clusterIdBuffer.writeCharSequence(clusterId, StandardCharsets.UTF_8);

    int authTokenSize = accessToken.readableBytes();

    ByteBuf byteBuf =
        buffer
            .writeInt(routerIdLength)
            .writeBytes(brokerIdBuffer)
            .writeInt(clusterIdLength)
            .writeBytes(clusterIdBuffer)
            .writeLong(accessKey)
            .writeInt(authTokenSize)
            .writeBytes(accessToken);

    ReferenceCountUtil.safeRelease(brokerIdBuffer);
    ReferenceCountUtil.safeRelease(clusterId);

    return byteBuf;
  }

  public static CharSequence brokerId(ByteBuf byteBuf) {
    byteBuf.resetReaderIndex();

    int offset = FrameHeaderFlyweight.size(byteBuf);
    byteBuf.readerIndex(offset);

    int length = byteBuf.readInt();

    return byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
  }

  public static CharSequence clusterId(ByteBuf byteBuf) {
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