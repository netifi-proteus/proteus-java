package io.netifi.proteus.frames;

import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class DestinationSetupFlyweightTest {
  @Test
  public void testEncoding() {

    ByteBuf accessToken = Unpooled.wrappedBuffer("access token".getBytes());
    Tags tags = Tags.of("destination", "destination");

    ByteBuf byteBuf =
        DestinationSetupFlyweight.encode(
            ByteBufAllocator.DEFAULT, "group", Long.MAX_VALUE, accessToken, tags);

    Assert.assertEquals("group", DestinationSetupFlyweight.group(byteBuf));
    Assert.assertEquals(Long.MAX_VALUE, DestinationSetupFlyweight.accessKey(byteBuf));
    Assert.assertTrue(
        ByteBufUtil.equals(accessToken, DestinationSetupFlyweight.accessToken(byteBuf)));
    Assert.assertEquals(tags, DestinationSetupFlyweight.tags(byteBuf));
  }
}
