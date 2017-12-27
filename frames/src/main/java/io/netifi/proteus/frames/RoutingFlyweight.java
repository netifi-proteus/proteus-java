package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** */
public class RoutingFlyweight {
  private static final int ACCESS_KEY_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int ROUTE_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int DESTINATION_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int WRAPPED_METADATA_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int TOKEN_SIZE = BitUtil.SIZE_OF_INT;

  private RoutingFlyweight() {}

  public static int computeLength(boolean token, String destination, ByteBuf route) {
    return computeLength(token, false, destination, route, Unpooled.EMPTY_BUFFER);
  }

  public static int computeLength(
      boolean token, String destination, ByteBuf route, ByteBuf wrappedMetadata) {
    return computeLength(token, true, destination, route, wrappedMetadata);
  }

  private static int computeLength(
      boolean token,
      boolean hasMetadata,
      String destination,
      ByteBuf route,
      ByteBuf wrappedMetadata) {
    int length = FrameHeaderFlyweight.computeFrameHeaderLength();

    if (token) {
      length += TOKEN_SIZE;
    }

    length +=
        ACCESS_KEY_SIZE
            + ROUTE_LENGTH_SIZE
            + route.readableBytes()
            + DESTINATION_LENGTH_SIZE
            + destination.length();

    if (hasMetadata) {
      length += WRAPPED_METADATA_LENGTH_SIZE + wrappedMetadata.readableBytes();
    }

    return length;
  }

  public static int encode(
      ByteBuf byteBuf,
      boolean hasToken,
      int token,
      long fromAccessKey,
      String fromDestination,
      long seqId,
      ByteBuf route) {
    return encode(
        byteBuf,
        hasToken,
        false,
        token,
        fromAccessKey,
        fromDestination,
        seqId,
        route,
        Unpooled.EMPTY_BUFFER);
  }

  public static int encode(
      ByteBuf byteBuf,
      boolean hasToken,
      int token,
      long fromAccessKey,
      String fromDestination,
      long seqId,
      ByteBuf route,
      ByteBuf wrappedMetadata) {
    return encode(
        byteBuf,
        hasToken,
        true,
        token,
        fromAccessKey,
        fromDestination,
        seqId,
        route,
        wrappedMetadata);
  }

  private static int encode(
      ByteBuf byteBuf,
      boolean hasToken,
      boolean hasMetadata,
      int token,
      long fromAccessKey,
      String fromDestination,
      long seqId,
      ByteBuf route,
      ByteBuf wrappedMetadata) {

    int routeLength = route.readableBytes();

    byte[] destinationBytes = fromDestination.getBytes(StandardCharsets.US_ASCII);
    int destinationLength = destinationBytes.length;
    if (destinationLength > 255) {
      throw new IllegalArgumentException("destination is longer then 255 characters");
    }

    Objects.requireNonNull(route, "routes must not be null");

    int flags = FrameHeaderFlyweight.encodeFlags(true, hasMetadata, false, false, hasToken);

    int offset = FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.ROUTE, flags, seqId);

    if (hasToken) {
      byteBuf.setInt(offset, token);
      offset += TOKEN_SIZE;
    }

    byteBuf.setLong(offset, fromAccessKey);
    offset += ACCESS_KEY_SIZE;

    byteBuf.setByte(offset, destinationLength);
    offset += DESTINATION_LENGTH_SIZE;

    byteBuf.setBytes(offset, destinationBytes);
    offset += destinationLength;

    byteBuf.setInt(offset, routeLength);
    offset += ROUTE_LENGTH_SIZE;

    byteBuf.setBytes(offset, route);
    offset += routeLength;

    if (hasMetadata) {
      int wrappedMetadataLength = wrappedMetadata.readableBytes();

      byteBuf.setInt(offset, wrappedMetadataLength);
      offset += WRAPPED_METADATA_LENGTH_SIZE;

      byteBuf.setBytes(offset, wrappedMetadata);
      offset += wrappedMetadataLength;
    }

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static int token(ByteBuf byteBuf) {
    if (FrameHeaderFlyweight.token(byteBuf)) {
      int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
      return byteBuf.getInt(offset);
    } else {
      throw new IllegalStateException("no token flag set");
    }
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0);

    return byteBuf.getLong(offset);
  }

  public static String destination(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0)
            + ACCESS_KEY_SIZE;
    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE;
    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static ByteBuf route(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0)
            + ACCESS_KEY_SIZE;
    int destinationLength = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE + destinationLength;

    int length = byteBuf.getInt(offset);
    offset += ROUTE_LENGTH_SIZE;

    return byteBuf.slice(offset, length);
  }

  public static ByteBuf wrappedMetadata(ByteBuf byteBuf) {
    if (!FrameHeaderFlyweight.hasMetadata(byteBuf)) {
      throw new IllegalStateException("request has no meta data");
    }

    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength()
            + (FrameHeaderFlyweight.token(byteBuf) ? TOKEN_SIZE : 0)
            + ACCESS_KEY_SIZE;
    int destinationLength = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE + destinationLength;

    int routeLength = byteBuf.getInt(offset);
    offset += ROUTE_LENGTH_SIZE + routeLength;

    int length = byteBuf.getInt(offset);
    offset += WRAPPED_METADATA_LENGTH_SIZE;

    return byteBuf.slice(offset, length);
  }
}
