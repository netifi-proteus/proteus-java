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

public class FrameHeaderFlyweight {

  // Protocol Version
  public static final short MAJOR_VERSION = 0;
  public static final short MINOR_VERSION = 1;

  private static final int MAJOR_VERSION_SIZE = Short.BYTES;
  private static final int MINOR_VERSION_SIZE = Short.BYTES;
  private static final int FRAME_TYPE_SIZE = Short.BYTES;

  public static final int BYTES = MAJOR_VERSION_SIZE + MINOR_VERSION_SIZE + FRAME_TYPE_SIZE;

  private FrameHeaderFlyweight() {}

  public static ByteBuf encodeFrameHeader(
      final ByteBufAllocator allocator,
      final short majorVersion,
      final short minorVersion,
      final FrameType type) {
    return allocator
        .buffer()
        .writeShort(majorVersion)
        .writeShort(minorVersion)
        .writeShort(type.getEncodedType());
  }

  public static ByteBuf encodeFrameHeader(final ByteBufAllocator allocator, final FrameType type) {
    return encodeFrameHeader(allocator, MAJOR_VERSION, MINOR_VERSION, type);
  }

  public static short majorVersion(ByteBuf byteBuf) {
    return byteBuf.getShort(0);
  }

  public static short minorVersion(ByteBuf byteBuf) {
    return byteBuf.getShort(MAJOR_VERSION_SIZE);
  }

  public static FrameType frameType(ByteBuf byteBuf) {
    short frameTypeId = byteBuf.getShort(MAJOR_VERSION_SIZE + MINOR_VERSION_SIZE);
    return FrameType.from(frameTypeId);
  }
}
