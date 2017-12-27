package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class AdminPresenceResultFlyweight {
  private static final int ACCOUNT_ID_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int ROUTER_ADDRESS_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int FOUND_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int DESTINATION_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int GROUP_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength(String routerAddress, String group, String destination) {
    return ACCOUNT_ID_SIZE
        + ROUTER_ADDRESS_LENGTH_SIZE
        + FOUND_SIZE
        + DESTINATION_LENGTH_SIZE
        + GROUP_LENGTH_SIZE
        + destination.length()
        + group.length()
        + routerAddress.length();
  }

  public static int encode(
      ByteBuf byteBuf,
      long accountId,
      boolean found,
      String routerAddress,
      String group,
      String destination) {
    int routerAddressLength = routerAddress.length();
    int groupLength = group.length();
    int destinationLength = destination.length();

    int offset = 0;

    byteBuf.setLong(offset, accountId);
    offset += ACCOUNT_ID_SIZE;

    byteBuf.setByte(offset, (found ? 1 : 0));
    offset += FOUND_SIZE;

    byteBuf.setInt(offset, routerAddressLength);
    offset += ROUTER_ADDRESS_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, routerAddress, StandardCharsets.US_ASCII);
    offset += routerAddressLength;

    byteBuf.setByte(offset, groupLength);
    offset += GROUP_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, group, StandardCharsets.US_ASCII);
    offset += groupLength;

    byteBuf.setByte(offset, destinationLength);
    offset += DESTINATION_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, destination, StandardCharsets.US_ASCII);
    offset += destinationLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static long accountId(ByteBuf byteBuf) {
    return byteBuf.getLong(0);
  }

  public static boolean found(ByteBuf byteBuf) {
    return byteBuf.getByte(ACCOUNT_ID_SIZE) == 1;
  }

  public static String routerAddress(ByteBuf byteBuf) {
    int offset = ACCOUNT_ID_SIZE + FOUND_SIZE;

    int length = byteBuf.getInt(offset);
    offset += ROUTER_ADDRESS_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static String group(ByteBuf byteBuf) {
    int offset = ACCOUNT_ID_SIZE + FOUND_SIZE;
    int routerAddressLength = byteBuf.getInt(offset);
    offset += routerAddressLength + ROUTER_ADDRESS_LENGTH_SIZE;

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static String destination(ByteBuf byteBuf) {
    int offset = ACCOUNT_ID_SIZE + FOUND_SIZE;
    int routerAddressLength = byteBuf.getInt(offset);
    offset += routerAddressLength + ROUTER_ADDRESS_LENGTH_SIZE;

    int groupLength = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE + groupLength;

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }
}
