package io.netifi.proteus.frames.admin;

import io.netty.buffer.ByteBuf;

/** */
public class AdminPresenceGroupFlyweight {
  public static int computeLength(String group) {
    return AdminFrameHeaderFlyweight.computeLength(group);
  }

  public static int encode(ByteBuf byteBuf, long seqId, long accountId, String group) {
    return AdminFrameHeaderFlyweight.encode(
        byteBuf, AdminFrameType.ADMIN_FRAME_PRESENCE_GROUP, seqId, accountId, group);
  }

  public static long accountId(ByteBuf byteBuf) {
    return AdminFrameHeaderFlyweight.accountId(byteBuf);
  }

  public static String group(ByteBuf byteBuf) {
    return AdminFrameHeaderFlyweight.group(byteBuf);
  }
}
