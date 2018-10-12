package io.netifi.proteus.frames;

import io.netifi.proteus.tags.DefaultTags;
import io.netifi.proteus.tags.Tags;
import io.netifi.proteus.tags.TagsCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class UnicastFlyweightTest {
  @Test
  public void testEncoding() {
    Tags fromTagsIn = new DefaultTags();
    fromTagsIn.add("destination", "fromDestination");
    fromTagsIn.add("group", "fromGroup");

    Tags toTagsIn = new DefaultTags();
    toTagsIn.add("destination", "toDestination");
    toTagsIn.add("group", "toGroup");

    ByteBuf fromTags = TagsCodec.encode(ByteBufAllocator.DEFAULT, fromTagsIn);
    ByteBuf toTags = TagsCodec.encode(ByteBufAllocator.DEFAULT, toTagsIn);
    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());

    ByteBuf byteBuf = UnicastFlyweight.encode(ByteBufAllocator.DEFAULT, fromTags, toTags, metadata);

    Tags fromTagsOut = TagsCodec.decode(UnicastFlyweight.fromTags(byteBuf));
    Tags toTagsOut = TagsCodec.decode(UnicastFlyweight.toTags(byteBuf));

    Assert.assertEquals(fromTagsIn, fromTagsOut);
    Assert.assertEquals(toTagsIn, toTagsOut);

    System.out.println(ByteBufUtil.prettyHexDump(UnicastFlyweight.metadata(byteBuf)));
    Assert.assertTrue(ByteBufUtil.equals(metadata, UnicastFlyweight.metadata(byteBuf)));
  }
}
