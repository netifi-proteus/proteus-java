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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class DestinationSetupFlyweightTest {

  InetAddress address;

  {
    try {
      address = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      address = InetAddress.getLoopbackAddress();
    }
  }

  @Test
  public void testEncoding() {

    ByteBuf accessToken = Unpooled.wrappedBuffer("access token".getBytes());
    UUID connectionId = UUID.randomUUID();
    short additionalFlags = 0b00000000_00000001;
    Tags tags = Tags.of("com.netifi.destination", "com.netifi.destination");

    ByteBuf byteBuf =
        DestinationSetupFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            address,
            "group",
            Long.MAX_VALUE,
            accessToken,
            connectionId,
            additionalFlags,
            tags);

    Assert.assertArrayEquals(
        address.getAddress(), DestinationSetupFlyweight.inetAddress(byteBuf).get().getAddress());
    Assert.assertEquals("group", DestinationSetupFlyweight.group(byteBuf));
    Assert.assertEquals(Long.MAX_VALUE, DestinationSetupFlyweight.accessKey(byteBuf));
    Assert.assertTrue(
        ByteBufUtil.equals(accessToken, DestinationSetupFlyweight.accessToken(byteBuf)));
    Assert.assertEquals(connectionId, DestinationSetupFlyweight.connectionId(byteBuf));
    Assert.assertTrue(additionalFlags == DestinationSetupFlyweight.additionalFlags(byteBuf));
    Assert.assertEquals(tags, DestinationSetupFlyweight.tags(byteBuf));
  }
}
