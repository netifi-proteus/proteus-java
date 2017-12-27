package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class RouteDestinationFlyweight {
  private static final int LAST_MASK = 0b1000_0000;
  private static final int REMOVE_FLAG = 0b0111_1111;
  private static final int ROUTE_TYPE_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int ACCOUNT_ID_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int DESTINATION_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int GROUP_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final IllegalStateException UNDEFINED_ROUTE_EXCEPTION =
      new IllegalStateException("undefined route");

  private RouteDestinationFlyweight() {}

  public static long accountId(ByteBuf byteBuf) {
    return byteBuf.getLong(ROUTE_TYPE_SIZE);
  }

  public static String destination(ByteBuf byteBuf) {
    int offset = ROUTE_TYPE_SIZE + ACCOUNT_ID_SIZE;
    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE;
    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static RouteType routeType(ByteBuf byteBuf) {
    int id = byteBuf.getByte(0) & REMOVE_FLAG;
    return RouteType.from(id);
  }

  public static String group(ByteBuf byteBuf) {
    int offset = ROUTE_TYPE_SIZE + ACCOUNT_ID_SIZE;

    if (routeType(byteBuf).hasDestination()) {
      int destinationLength = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
      offset += DESTINATION_LENGTH_SIZE + destinationLength;
    }

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static int computeLength(RouteType routeType, String group) {
    if (routeType.hasDestination()) {
      throw new IllegalStateException(routeType + " excepts a destination");
    }

    return computeLength(routeType, null, group);
  }

  public static int computeLength(RouteType routeType, String destination, String group) {
    return ROUTE_TYPE_SIZE
        + ACCOUNT_ID_SIZE
        + (routeType.hasDestination() ? destination.length() + DESTINATION_LENGTH_SIZE : 0)
        + group.length()
        + GROUP_LENGTH_SIZE;
  }

  public static int encodeRouteByDestination(
      ByteBuf byteBuf, RouteType routeType, long accountId, String destination, String group) {
    return encode(byteBuf, routeType, accountId, destination, group);
  }

  public static int encodeRouteByGroup(
      ByteBuf byteBuf, RouteType routeType, long accountId, String group) {
    return encode(byteBuf, routeType, accountId, null, group);
  }

  private static int encode(
      ByteBuf byteBuf, RouteType routeType, long accountId, String destination, String group) {

    boolean hasDestination = routeType.hasDestination();
    int destinationLength = hasDestination ? destination.length() : 0;
    int groupLength = group.length();

    if (hasDestination && destinationLength > 255) {
      throw new IllegalArgumentException("destination is longer then 255 characters");
    }

    if (groupLength > 255) {
      throw new IllegalArgumentException("group is longer then 255 characters");
    }

    int offset = 0;
    int encodedType = routeType.getEncodedType();

    byteBuf.setByte(offset, encodedType);
    offset += ROUTE_TYPE_SIZE;

    byteBuf.setLong(offset, accountId);
    offset += ACCOUNT_ID_SIZE;

    if (hasDestination) {
      byteBuf.setByte(offset, destinationLength);
      offset += DESTINATION_LENGTH_SIZE;

      byte[] bytes = destination.getBytes(StandardCharsets.US_ASCII);
      byteBuf.setBytes(offset, bytes);
      offset += destinationLength;
    }

    byteBuf.setByte(offset, groupLength);
    offset += GROUP_LENGTH_SIZE;

    byte[] bytes = group.getBytes(StandardCharsets.US_ASCII);
    byteBuf.setBytes(offset, bytes);
    offset += groupLength;

    byteBuf.writerIndex(offset);

    return offset;
  }
}
