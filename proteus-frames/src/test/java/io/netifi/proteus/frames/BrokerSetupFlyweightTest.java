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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class BrokerSetupFlyweightTest {
  @Test
  public void testEncoding() {
    ByteBuf authToken = Unpooled.wrappedBuffer("access token".getBytes());

    ByteBuf byteBuf =
        BrokerSetupFlyweight.encode(
            ByteBufAllocator.DEFAULT, "brokerId", "clusterId", Long.MAX_VALUE, authToken);

    Assert.assertEquals("brokerId", BrokerSetupFlyweight.brokerId(byteBuf));
    Assert.assertEquals("clusterId", BrokerSetupFlyweight.clusterId(byteBuf));
    Assert.assertEquals(Long.MAX_VALUE, BrokerSetupFlyweight.accessKey(byteBuf));
    authToken.resetReaderIndex();
    Assert.assertTrue(ByteBufUtil.equals(authToken, BrokerSetupFlyweight.accessToken(byteBuf)));
  }
}
