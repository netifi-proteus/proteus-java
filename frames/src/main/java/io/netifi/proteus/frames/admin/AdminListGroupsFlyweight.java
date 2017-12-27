package io.netifi.proteus.frames.admin;

import io.netty.buffer.ByteBuf;

/** */
public class AdminListGroupsFlyweight {
  public static int computeLength() {
    return AdminFrameHeaderFlyweight.computeLength("");
  }

  public static int encode(ByteBuf byteBuf, long seqId) {
    return AdminFrameHeaderFlyweight.encode(
        byteBuf, AdminFrameType.ADMIN_FRAME_LIST_GROUPS, seqId, -1, "");
  }
}
