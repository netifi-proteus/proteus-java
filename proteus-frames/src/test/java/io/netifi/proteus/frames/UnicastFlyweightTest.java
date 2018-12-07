package io.netifi.proteus.frames;

import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class UnicastFlyweightTest {
  @Test
  public void testEncoding() {

    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
    Tags tags = Tags.of("destination", "toDestination");
    ByteBuf byteBuf = UnicastFlyweight.encode(ByteBufAllocator.DEFAULT, "group", metadata, tags);

    System.out.println(ByteBufUtil.prettyHexDump(UnicastFlyweight.metadata(byteBuf)));
    Assert.assertEquals("group", UnicastFlyweight.group(byteBuf));
    Assert.assertTrue(ByteBufUtil.equals(metadata, UnicastFlyweight.metadata(byteBuf)));
    Assert.assertEquals(tags, UnicastFlyweight.tags(byteBuf));
  }
}
