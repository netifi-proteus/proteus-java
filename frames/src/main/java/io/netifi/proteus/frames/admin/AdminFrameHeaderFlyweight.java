package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netifi.proteus.frames.FrameHeaderFlyweight;
import io.netifi.proteus.frames.FrameType;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class AdminFrameHeaderFlyweight {
  private static final int ADMIN_FRAME_TYPE = BitUtil.SIZE_OF_BYTE;
  private static final int ACCOUNT_ID_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int GROUP_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength(String group) {
    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + ADMIN_FRAME_TYPE
        + ACCOUNT_ID_SIZE
        + GROUP_LENGTH_SIZE
        + group.length();
  }

  public static int encode(
      ByteBuf byteBuf, AdminFrameType type, long seqId, long accountId, String group) {
    int groupLength = group.length();

    if (groupLength > 255) {
      throw new IllegalArgumentException("group is longer then 255 characters");
    }

    int offset =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, FrameType.EXTENSION_FRAME, 0, seqId);

    byteBuf.setByte(offset, type.getEncodedType());
    offset += ADMIN_FRAME_TYPE;

    byteBuf.setLong(offset, accountId);
    offset += ACCOUNT_ID_SIZE;

    byteBuf.setByte(offset, groupLength);
    offset += GROUP_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, group, StandardCharsets.US_ASCII);
    offset += groupLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static AdminFrameType adminFrameType(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    byte aByte = byteBuf.getByte(offset);
    return AdminFrameType.from(aByte);
  }

  public static long accountId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + ADMIN_FRAME_TYPE;
    return byteBuf.getLong(offset);
  }

  public static String group(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.computeFrameHeaderLength() + ADMIN_FRAME_TYPE + ACCOUNT_ID_SIZE;

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }
}
