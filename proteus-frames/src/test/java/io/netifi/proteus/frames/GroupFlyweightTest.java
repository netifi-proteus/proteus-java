package io.netifi.proteus.frames;

import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class GroupFlyweightTest {
  @Test
  public void testEncoding() {

    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
    Tags tags = Tags.of("destination", "toDestination");
    ByteBuf byteBuf = GroupFlyweight.encode(ByteBufAllocator.DEFAULT, "group", metadata, tags);

    System.out.println(ByteBufUtil.prettyHexDump(GroupFlyweight.metadata(byteBuf)));
    Assert.assertEquals("group", GroupFlyweight.group(byteBuf));
    Assert.assertTrue(ByteBufUtil.equals(metadata, GroupFlyweight.metadata(byteBuf)));
    Assert.assertEquals(tags, GroupFlyweight.tags(byteBuf));
  }
}
