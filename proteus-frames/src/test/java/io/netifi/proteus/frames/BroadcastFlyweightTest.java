package io.netifi.proteus.frames;

import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class BroadcastFlyweightTest {

  @Test
  public void testEncoding() {
    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
    Tags tags = Tags.of("tag", "tag");
    ByteBuf byteBuf = BroadcastFlyweight.encode(ByteBufAllocator.DEFAULT, "group", metadata, tags);

    Assert.assertEquals("group", BroadcastFlyweight.group(byteBuf));
    System.out.println(ByteBufUtil.prettyHexDump(BroadcastFlyweight.metadata(byteBuf)));
    Assert.assertTrue(ByteBufUtil.equals(metadata, BroadcastFlyweight.metadata(byteBuf)));
    Assert.assertEquals(tags, BroadcastFlyweight.tags(byteBuf));
  }
}
