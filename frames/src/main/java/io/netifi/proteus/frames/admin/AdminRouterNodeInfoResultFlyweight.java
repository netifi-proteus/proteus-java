package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

public class AdminRouterNodeInfoResultFlyweight {
  private static final int ROUTER_ID_LENGTH_SIZE = BitUtil.SIZE_OF_INT;

  private static final int ROUTER_ADDRESS_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int ROUTER_PORT_SIZE = BitUtil.SIZE_OF_INT;

  private static final int CLUSTER_ADDRESS_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int CLUSTER_PORT_SIZE = BitUtil.SIZE_OF_INT;

  private static final int ADMIN_ADDRESS_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int ADMIN_PORT_SIZE = BitUtil.SIZE_OF_INT;

  private static final int NODE_INFO_EVENT_TYPE_SIZE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength(
      String routerId, String routerAddress, String clusterAddress, String adminAddress) {
    return ROUTER_ID_LENGTH_SIZE
        + ROUTER_ADDRESS_LENGTH_SIZE
        + ROUTER_PORT_SIZE
        + CLUSTER_ADDRESS_LENGTH_SIZE
        + CLUSTER_PORT_SIZE
        + ADMIN_ADDRESS_LENGTH_SIZE
        + ADMIN_PORT_SIZE
        + NODE_INFO_EVENT_TYPE_SIZE
        + routerId.length()
        + routerAddress.length()
        + clusterAddress.length()
        + adminAddress.length();
  }

  public static int encode(
      ByteBuf byteBuf,
      AdminRouterNodeInfoEventType eventType,
      String routerId,
      String routerAddress,
      int routerPort,
      String clusterAddress,
      int clusterPort,
      String adminAddress,
      int adminPort) {

    int routerIdLength = routerId.length();
    int routerAddressLength = routerAddress.length();
    int clusterAddressLength = clusterAddress.length();
    int adminAddressLength = adminAddress.length();

    int offset = 0;

    byteBuf.setByte(offset, eventType.getEncodedType());
    offset += NODE_INFO_EVENT_TYPE_SIZE;

    // Router Id
    byteBuf.setInt(offset, routerIdLength);
    offset += ROUTER_ID_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, routerId, StandardCharsets.US_ASCII);
    offset += routerIdLength;

    // Router Address and Port
    byteBuf.setInt(offset, routerAddressLength);
    offset += ROUTER_ADDRESS_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, routerAddress, StandardCharsets.US_ASCII);
    offset += routerAddressLength;

    byteBuf.setInt(offset, routerPort);
    offset += ROUTER_PORT_SIZE;

    // Cluster Address and Port
    byteBuf.setInt(offset, clusterAddressLength);
    offset += CLUSTER_ADDRESS_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, clusterAddress, StandardCharsets.US_ASCII);
    offset += clusterAddressLength;

    byteBuf.setInt(offset, clusterPort);
    offset += CLUSTER_PORT_SIZE;

    // Admin Address and Port
    byteBuf.setInt(offset, adminAddressLength);
    offset += ADMIN_ADDRESS_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, adminAddress, StandardCharsets.US_ASCII);
    offset += adminAddressLength;

    byteBuf.setInt(offset, adminPort);
    offset += ADMIN_PORT_SIZE;

    byteBuf.writerIndex(offset);
    return offset;
  }

  public static AdminRouterNodeInfoEventType eventType(ByteBuf byteBuf) {
    byte aByte = byteBuf.getByte(0);
    return AdminRouterNodeInfoEventType.from(aByte);
  }

  public static String routerId(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    int length = byteBuf.getInt(offset);
    offset += ROUTER_ID_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static String routerAddress(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    offset += (byteBuf.getInt(offset) + ROUTER_ID_LENGTH_SIZE);
    int length = byteBuf.getInt(offset);
    offset += ROUTER_ADDRESS_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static int routerPort(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    offset += (byteBuf.getInt(offset) + ROUTER_ID_LENGTH_SIZE);
    offset += (byteBuf.getInt(offset) + ROUTER_ADDRESS_LENGTH_SIZE);

    return byteBuf.getInt(offset);
  }

  public static String clusterAddress(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    offset += (byteBuf.getInt(offset) + ROUTER_ID_LENGTH_SIZE);
    offset += (byteBuf.getInt(offset) + ROUTER_ADDRESS_LENGTH_SIZE + ROUTER_PORT_SIZE);

    int length = byteBuf.getInt(offset);
    offset += CLUSTER_ADDRESS_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static int clusterPort(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    offset += (byteBuf.getInt(offset) + ROUTER_ID_LENGTH_SIZE);
    offset += (byteBuf.getInt(offset) + ROUTER_ADDRESS_LENGTH_SIZE + ROUTER_PORT_SIZE);
    offset += (byteBuf.getInt(offset) + CLUSTER_ADDRESS_LENGTH_SIZE);

    return byteBuf.getInt(offset);
  }

  public static String adminAddress(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    offset += (byteBuf.getInt(offset) + ROUTER_ID_LENGTH_SIZE);
    offset += (byteBuf.getInt(offset) + ROUTER_ADDRESS_LENGTH_SIZE + ROUTER_PORT_SIZE);
    offset += (byteBuf.getInt(offset) + CLUSTER_ADDRESS_LENGTH_SIZE + CLUSTER_PORT_SIZE);

    int length = byteBuf.getInt(offset);
    offset += ADMIN_ADDRESS_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static int adminPort(ByteBuf byteBuf) {
    int offset = NODE_INFO_EVENT_TYPE_SIZE;
    offset += (byteBuf.getInt(offset) + ROUTER_ID_LENGTH_SIZE);
    offset += (byteBuf.getInt(offset) + ROUTER_ADDRESS_LENGTH_SIZE + ROUTER_PORT_SIZE);
    offset += (byteBuf.getInt(offset) + CLUSTER_ADDRESS_LENGTH_SIZE + CLUSTER_PORT_SIZE);
    offset += (byteBuf.getInt(offset) + ADMIN_ADDRESS_LENGTH_SIZE);

    return byteBuf.getInt(offset);
  }
}
