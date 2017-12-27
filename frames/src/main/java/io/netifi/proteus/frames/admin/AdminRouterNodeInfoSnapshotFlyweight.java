package io.netifi.proteus.frames.admin;

import io.netty.buffer.ByteBuf;

public class AdminRouterNodeInfoSnapshotFlyweight {
  public static int computeLength() {
    return AdminFrameHeaderFlyweight.computeLength("");
  }

  public static int encode(ByteBuf byteBuf, long seqId) {
    return AdminFrameHeaderFlyweight.encode(
        byteBuf, AdminFrameType.ADMIN_FRAME_ROUTER_NODE_INFO_SNAPSHOT, seqId, -1, "");
  }
}
