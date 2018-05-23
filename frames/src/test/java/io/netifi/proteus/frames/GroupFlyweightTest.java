package io.netifi.proteus.frames;

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
    ByteBuf byteBuf =
        GroupFlyweight.encode(
            ByteBufAllocator.DEFAULT, "fromDestination", "fromGroup", "toGroup", metadata);

    Assert.assertEquals("fromDestination", GroupFlyweight.fromDestination(byteBuf));
    Assert.assertEquals("fromGroup", GroupFlyweight.fromGroup(byteBuf));
    Assert.assertEquals("toGroup", GroupFlyweight.toGroup(byteBuf));
    metadata.resetReaderIndex();
    Assert.assertTrue(ByteBufUtil.equals(metadata, GroupFlyweight.metadata(byteBuf)));
  }
}
