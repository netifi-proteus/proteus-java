package io.netifi.proteus.frames.admin;

import io.netifi.proteus.frames.BitUtil;
import io.netifi.proteus.frames.FrameHeaderFlyweight;
import io.netifi.proteus.frames.FrameType;
import io.netty.buffer.ByteBuf;

/** */
public class AdminSetupFlyweight {
  private static final int ADMIN_FRAME_TYPE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength() {
    return FrameHeaderFlyweight.computeFrameHeaderLength() + ADMIN_FRAME_TYPE;
  }

  public static int encode(ByteBuf byteBuf) {
    int offset =
        FrameHeaderFlyweight.encodeFrameHeader(
            byteBuf, FrameType.EXTENSION_FRAME, 0, System.currentTimeMillis());

    byteBuf.setByte(offset, AdminFrameType.ADMIN_SETUP_FRAME.getEncodedType());
    offset += ADMIN_FRAME_TYPE;

    byteBuf.writerIndex(offset);

    return offset;
  }
}
