/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
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
