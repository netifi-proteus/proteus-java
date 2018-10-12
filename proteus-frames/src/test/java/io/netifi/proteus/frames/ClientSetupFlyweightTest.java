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

public class ClientSetupFlyweightTest {
  @Test
  public void testEncoding() {

    ByteBuf accessToken = Unpooled.wrappedBuffer("access token".getBytes());

    Tags tagsIn = new DefaultTags();
    tagsIn.add("destination", "destination");
    tagsIn.add("group", "group");

    ByteBuf tags = TagsCodec.encode(ByteBufAllocator.DEFAULT, tagsIn);

    ByteBuf byteBuf =
        ClientSetupFlyweight.encode(ByteBufAllocator.DEFAULT, Long.MAX_VALUE, accessToken, tags);

    Assert.assertEquals(Long.MAX_VALUE, ClientSetupFlyweight.accessKey(byteBuf));
    Assert.assertTrue(ByteBufUtil.equals(accessToken, ClientSetupFlyweight.accessToken(byteBuf)));

    Tags tagsOut = TagsCodec.decode(ClientSetupFlyweight.tags(byteBuf));

    Assert.assertEquals(tagsIn, tagsOut);
  }
}
