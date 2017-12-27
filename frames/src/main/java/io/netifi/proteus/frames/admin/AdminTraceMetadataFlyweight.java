package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class AdminTraceMetadataFlyweight {
  private static final int ADMIN_TRACE_TYPE_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int ROUTER_ADDRESS_LENGTH_SIZE = BitUtil.SIZE_OF_INT;
  private static final int GROUP_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int METRIC_SIZE = BitUtil.SIZE_OF_DOUBLE;

  public static int computeLength(String routerAddress, String group) {
    return ADMIN_TRACE_TYPE_SIZE
        + METRIC_SIZE
        + ROUTER_ADDRESS_LENGTH_SIZE
        + GROUP_LENGTH_SIZE
        + routerAddress.length()
        + group.length();
  }

  public static int encode(
      ByteBuf byteBuf, AdminTraceType type, String routerAddress, String group, double metric) {
    int routerAddressLength = routerAddress.length();
    int groupLength = group.length();

    int offset = 0;

    byteBuf.setByte(offset, type.getEncodedType());
    offset += ADMIN_TRACE_TYPE_SIZE;

    byteBuf.setDouble(offset, metric);
    offset += METRIC_SIZE;

    byteBuf.setInt(offset, routerAddressLength);
    offset += ROUTER_ADDRESS_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, routerAddress, StandardCharsets.US_ASCII);
    offset += routerAddressLength;

    byteBuf.setByte(offset, groupLength);
    offset += GROUP_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, group, StandardCharsets.US_ASCII);
    offset += groupLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static AdminTraceType adminTraceType(ByteBuf byteBuf) {
    int id = BitUtil.toUnsignedInt(byteBuf.getByte(0));
    return AdminTraceType.from(id);
  }

  public static double metric(ByteBuf byteBuf) {
    int offset = ADMIN_TRACE_TYPE_SIZE;
    return byteBuf.getDouble(offset);
  }

  public static String routerAddress(ByteBuf byteBuf) {
    int offset = ADMIN_TRACE_TYPE_SIZE + METRIC_SIZE;

    int length = byteBuf.getInt(offset);
    offset += ROUTER_ADDRESS_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static String group(ByteBuf byteBuf) {
    int offset = ADMIN_TRACE_TYPE_SIZE + METRIC_SIZE;

    int routerAddressLength = byteBuf.getInt(offset);
    offset += ROUTER_ADDRESS_LENGTH_SIZE + routerAddressLength;

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }
}
