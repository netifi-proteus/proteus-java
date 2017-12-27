package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class AdminListGroupsResultFlyweight {
  private static final int ACCOUNT_ID_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int GROUP_MEMBER_SIZE_SIZE = BitUtil.SIZE_OF_INT;
  private static final int GROUP_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength(String group) {
    return ACCOUNT_ID_SIZE + GROUP_MEMBER_SIZE_SIZE + GROUP_LENGTH_SIZE + group.length();
  }

  public static int encode(ByteBuf byteBuf, long accountId, int size, String group) {
    int groupLength = group.length();

    int offset = 0;

    byteBuf.setLong(offset, accountId);
    offset += ACCOUNT_ID_SIZE;

    byteBuf.setInt(offset, size);
    offset += GROUP_MEMBER_SIZE_SIZE;

    byteBuf.setByte(offset, groupLength);
    offset += GROUP_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, group, StandardCharsets.US_ASCII);
    offset += groupLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static long accountId(ByteBuf byteBuf) {
    return byteBuf.getLong(0);
  }

  public static int size(ByteBuf byteBuf) {
    return byteBuf.getInt(ACCOUNT_ID_SIZE);
  }

  public static String group(ByteBuf byteBuf) {
    int offset = ACCOUNT_ID_SIZE + GROUP_MEMBER_SIZE_SIZE;

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }
}
