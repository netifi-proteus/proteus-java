package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

public class FrameHeaderFlyweightTest {
  @Test
  public void testEncoding() {
    short major = 50;
    short minor = 50;
    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(
            ByteBufAllocator.DEFAULT, major, minor, FrameType.CLIENT_SETUP);

    Assert.assertEquals(major, FrameHeaderFlyweight.majorVersion(byteBuf));
    Assert.assertEquals(minor, FrameHeaderFlyweight.minorVersion(byteBuf));
    Assert.assertEquals(FrameType.CLIENT_SETUP, FrameHeaderFlyweight.frameType(byteBuf));
  }
}
