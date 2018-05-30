package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class BrokerSetupFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence brokerId,
      CharSequence clusterId,
      long accessKey,
      ByteBuf accessToken) {

    Objects.requireNonNull(brokerId);
    Objects.requireNonNull(clusterId);

    ByteBuf brokerIdBuffer = ByteBufUtil.writeUtf8(allocator, brokerId);
    ByteBuf clusterIdBuffer = ByteBufUtil.writeUtf8(allocator, clusterId);

    int authTokenSize = accessToken.readableBytes();

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.BROKER_SETUP)
            .writeInt(brokerIdBuffer.readableBytes())
            .writeBytes(brokerIdBuffer)
            .writeInt(clusterIdBuffer.readableBytes())
            .writeBytes(clusterIdBuffer)
            .writeLong(accessKey)
            .writeInt(authTokenSize)
            .writeBytes(accessToken, accessToken.readerIndex(), authTokenSize);

    ReferenceCountUtil.safeRelease(brokerIdBuffer);
    ReferenceCountUtil.safeRelease(clusterId);

    return byteBuf;
  }

  public static CharSequence brokerId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, brokerIdLength, StandardCharsets.UTF_8);
  }

  public static CharSequence clusterId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + brokerIdLength;

    int clusterIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.getCharSequence(offset, clusterIdLength, StandardCharsets.UTF_8);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + brokerIdLength;

    int clusterIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + clusterIdLength;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.size(byteBuf);

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + brokerIdLength;

    int clusterIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + clusterIdLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, accessTokenLength);
  }
}
