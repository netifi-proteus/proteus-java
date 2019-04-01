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
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class BrokerSetupFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      CharSequence brokerId,
      CharSequence clusterId,
      long accessKey,
      ByteBuf accessToken) {

    Objects.requireNonNull(brokerId);
    Objects.requireNonNull(clusterId);

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.BROKER_SETUP);

    int brokerIdLength = ByteBufUtil.utf8Bytes(brokerId);
    byteBuf.writeInt(brokerIdLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, brokerId, brokerIdLength);

    int clusterIdLength = ByteBufUtil.utf8Bytes(clusterId);
    byteBuf.writeInt(clusterIdLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, clusterId, clusterIdLength);

    int authTokenSize = accessToken.readableBytes();
    byteBuf
        .writeLong(accessKey)
        .writeInt(authTokenSize)
        .writeBytes(accessToken, accessToken.readerIndex(), authTokenSize);

    return byteBuf;
  }

  public static String brokerId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, brokerIdLength, StandardCharsets.UTF_8);
  }

  public static String clusterId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + brokerIdLength;

    int clusterIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, clusterIdLength, StandardCharsets.UTF_8);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + brokerIdLength;

    int clusterIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + clusterIdLength;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int brokerIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + brokerIdLength;

    int clusterIdLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + clusterIdLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, accessTokenLength);
  }
}
