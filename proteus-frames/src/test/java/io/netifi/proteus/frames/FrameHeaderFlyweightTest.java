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
import org.junit.Assert;
import org.junit.Test;

public class FrameHeaderFlyweightTest {
  @Test
  public void testEncoding() {
    short major = 50;
    short minor = 50;
    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(
            ByteBufAllocator.DEFAULT, major, minor, FrameType.DESTINATION_SETUP);

    Assert.assertEquals(major, FrameHeaderFlyweight.majorVersion(byteBuf));
    Assert.assertEquals(minor, FrameHeaderFlyweight.minorVersion(byteBuf));
    Assert.assertEquals(FrameType.DESTINATION_SETUP, FrameHeaderFlyweight.frameType(byteBuf));
  }
}
