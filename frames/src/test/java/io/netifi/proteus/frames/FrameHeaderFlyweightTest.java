package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** */
public class FrameHeaderFlyweightTest {
  @Test
  @Ignore
  public void testEncodeHeader() {
    FrameType channelAckedGroupRoute = FrameType.DESTINATION_AVAIL_RESULT;
    byte flags = 1;
    byte majorVersion = 12;
    byte minorVersion = 14;
    int expectedValue = 134286350;

    ByteBuf buf = Unpooled.buffer(4);

    FrameHeaderFlyweight.encodeFrameHeader(
        buf, channelAckedGroupRoute, flags, majorVersion, minorVersion, 0);

    Assert.assertEquals(expectedValue, buf.getInt(0));
  }

  @Test
  @Ignore
  public void testDecodeHeader() {
    ByteBuf buf = Unpooled.buffer(4);
    buf.setInt(0, 134286350);
    Assert.assertEquals(FrameHeaderFlyweight.frameType(buf), FrameType.DESTINATION_AVAIL_RESULT);
    Assert.assertEquals(FrameHeaderFlyweight.flags(buf), 1);
    Assert.assertEquals(FrameHeaderFlyweight.majorVersion(buf), 12);
    Assert.assertEquals(FrameHeaderFlyweight.minorVersion(buf), 14);
  }

  @Test
  public void testFlags() {
    ByteBuf buf = Unpooled.buffer(4);
    buf.setInt(0, 216009742);

    Assert.assertTrue(FrameHeaderFlyweight.hasData(buf));
    Assert.assertTrue(FrameHeaderFlyweight.hasMetadata(buf));
    Assert.assertTrue(FrameHeaderFlyweight.encrypted(buf));
    Assert.assertFalse(FrameHeaderFlyweight.broadcast(buf));
    Assert.assertFalse(FrameHeaderFlyweight.apiCall(buf));
    Assert.assertFalse(FrameHeaderFlyweight.token(buf));
  }
}
