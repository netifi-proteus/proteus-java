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

import io.netifi.proteus.common.tags.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class AuthorizationWrapperFlyweightTest {

  private static long ACCESS_KEY = 123456789L;

  @Test
  public void testEncodingBroadcast() {

    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
    Tags tags = Tags.of("tag", "tag");
    ByteBuf frame = BroadcastFlyweight.encode(ByteBufAllocator.DEFAULT, "group", metadata, tags);

    ByteBuf wrappedByteBuf =
        AuthorizationWrapperFlyweight.encode(ByteBufAllocator.DEFAULT, ACCESS_KEY, frame);

    ByteBuf byteBuf = AuthorizationWrapperFlyweight.innerFrame(wrappedByteBuf);

    Assert.assertEquals(123456789L, AuthorizationWrapperFlyweight.accessKey(wrappedByteBuf));

    Assert.assertEquals("group", BroadcastFlyweight.group(byteBuf));
    Assert.assertEquals("group", BroadcastFlyweight.group(byteBuf));
    System.out.println(ByteBufUtil.prettyHexDump(BroadcastFlyweight.metadata(byteBuf)));
    Assert.assertTrue(ByteBufUtil.equals(metadata, BroadcastFlyweight.metadata(byteBuf)));
    Assert.assertEquals(tags, BroadcastFlyweight.tags(byteBuf));
  }

  @Test
  public void testEncodingGroup() {

    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
    Tags tags = Tags.of("com.netifi.destination", "toDestination");
    ByteBuf frame = GroupFlyweight.encode(ByteBufAllocator.DEFAULT, "group", metadata, tags);

    ByteBuf wrappedByteBuf =
        AuthorizationWrapperFlyweight.encode(ByteBufAllocator.DEFAULT, ACCESS_KEY, frame);

    ByteBuf byteBuf = AuthorizationWrapperFlyweight.innerFrame(wrappedByteBuf);

    Assert.assertEquals(123456789L, AuthorizationWrapperFlyweight.accessKey(wrappedByteBuf));

    System.out.println(ByteBufUtil.prettyHexDump(GroupFlyweight.metadata(byteBuf)));
    Assert.assertEquals("group", GroupFlyweight.group(byteBuf));
    Assert.assertTrue(ByteBufUtil.equals(metadata, GroupFlyweight.metadata(byteBuf)));
    Assert.assertEquals(tags, GroupFlyweight.tags(byteBuf));
  }

  @Test
  public void testEncodingShard() {
    ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
    ByteBuf shardKey = Unpooled.wrappedBuffer("shardKey".getBytes());
    Tags tags = Tags.of("tag", "tag");
    ByteBuf frame =
        ShardFlyweight.encode(ByteBufAllocator.DEFAULT, "group", metadata, shardKey, tags);

    ByteBuf wrappedByteBuf =
        AuthorizationWrapperFlyweight.encode(ByteBufAllocator.DEFAULT, ACCESS_KEY, frame);

    ByteBuf byteBuf = AuthorizationWrapperFlyweight.innerFrame(wrappedByteBuf);

    Assert.assertEquals(123456789L, AuthorizationWrapperFlyweight.accessKey(wrappedByteBuf));

    Assert.assertEquals("group", ShardFlyweight.group(byteBuf));
    System.out.println(ByteBufUtil.prettyHexDump(ShardFlyweight.metadata(byteBuf)));
    Assert.assertTrue(ByteBufUtil.equals(metadata, ShardFlyweight.metadata(byteBuf)));
    Assert.assertTrue(ByteBufUtil.equals(shardKey, ShardFlyweight.shardKey(byteBuf)));
    Assert.assertEquals(tags, ShardFlyweight.tags(byteBuf));
  }
}
