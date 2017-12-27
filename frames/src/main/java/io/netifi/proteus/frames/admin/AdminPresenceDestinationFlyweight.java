package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class AdminPresenceDestinationFlyweight {
  private static final int DESTINATION_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength(String group, String destination) {
    return AdminFrameHeaderFlyweight.computeLength(group)
        + DESTINATION_LENGTH_SIZE
        + destination.length();
  }

  public static int encode(
      ByteBuf byteBuf, long seqId, long accountId, String group, String destination) {

    int destinationLength = destination.length();
    int offset =
        AdminFrameHeaderFlyweight.encode(
            byteBuf, AdminFrameType.ADMIN_FRAME_PRESENCE_DESTINATION, seqId, accountId, group);

    byteBuf.setByte(offset, destinationLength);
    offset += DESTINATION_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, destination, StandardCharsets.US_ASCII);
    offset += destinationLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static long accountId(ByteBuf byteBuf) {
    return AdminFrameHeaderFlyweight.accountId(byteBuf);
  }

  public static String group(ByteBuf byteBuf) {
    return AdminFrameHeaderFlyweight.group(byteBuf);
  }

  public static String destination(ByteBuf byteBuf) {
    int offset = AdminPresenceGroupFlyweight.computeLength(group(byteBuf));
    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }
}
