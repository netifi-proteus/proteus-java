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

public class AuthorizationWrapperFlyweight {

  public static ByteBuf encode(ByteBufAllocator allocator, long accessKey, ByteBuf innerFrame) {

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.AUTHORIZATION_WRAPPER);

    byteBuf.writeLong(accessKey).writeBytes(innerFrame);

    return byteBuf;
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf innerFrame(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;
    offset += Long.BYTES;

    byteBuf.markReaderIndex();
    byteBuf.skipBytes(offset);
    ByteBuf slice = byteBuf.slice();
    byteBuf.resetReaderIndex();
    return slice;
  }
}
