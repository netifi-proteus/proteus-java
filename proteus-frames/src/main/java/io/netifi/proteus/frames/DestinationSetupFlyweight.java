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

import io.netifi.proteus.common.tags.Tag;
import io.netifi.proteus.common.tags.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DestinationSetupFlyweight {
  private static final int CONNECTION_ID_LENGTH = Long.BYTES + Long.BYTES;
  private static final int ADDITIONAL_FLAGS_SIZE = Short.BYTES;
  public static final short FLAG_ENABLE_PUBLIC_ACCESS = 0b00000000_00000001;
  
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      InetAddress inetAddress,
      CharSequence group,
      long accessKey,
      byte[] accessToken,
      UUID connectionId,
      Tags tags) {
    return encode(
        allocator,
        inetAddress,
        group,
        accessKey,
        Unpooled.wrappedBuffer(accessToken),
        connectionId,
        (short) 0,
        tags);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      InetAddress inetAddress,
      CharSequence group,
      long accessKey,
      ByteBuf accessToken,
      UUID connectionId,
      Tags tags) {
    return encode(
        allocator, inetAddress, group, accessKey, accessToken, connectionId, (short) 0, tags);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      InetAddress inetAddress,
      CharSequence group,
      long accessKey,
      byte[] accessToken,
      UUID connectionId,
      short additionalFlags,
      Tags tags) {
    return encode(
        allocator,
        inetAddress,
        group,
        accessKey,
        Unpooled.wrappedBuffer(accessToken),
        connectionId,
        additionalFlags,
        tags);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      InetAddress inetAddress,
      CharSequence group,
      long accessKey,
      ByteBuf accessToken,
      UUID connectionId,
      short additionalFlags,
      Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);

    ByteBuf byteBuf =
        FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.DESTINATION_SETUP);

    if (inetAddress != null) { // backward compatibility + specific non-inet transports support
      byte[] addressBytes = inetAddress.getAddress();
      byteBuf.writeInt(addressBytes.length).writeBytes(addressBytes);
    } else {
      byteBuf.writeInt(0);
    }

    int groupLength = ByteBufUtil.utf8Bytes(group);
    byteBuf.writeInt(groupLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, group, groupLength);

    int accessTokenLength = accessToken.readableBytes();
    byteBuf
        .writeLong(accessKey)
        .writeInt(accessTokenLength)
        .writeBytes(accessToken, accessToken.readerIndex(), accessTokenLength);

    byteBuf
        .writeLong(connectionId.getMostSignificantBits())
        .writeLong(connectionId.getLeastSignificantBits());

    // Additional flags, currently just 00000000_00000000 or 00000000_00000001 for private/public
    // services
    byteBuf.writeShort(additionalFlags);

    for (Tag tag : tags) {
      String key = tag.getKey();
      String value = tag.getValue();

      int keyLength = ByteBufUtil.utf8Bytes(key);
      byteBuf.writeInt(keyLength);
      ByteBufUtil.reserveAndWriteUtf8(byteBuf, key, keyLength);

      int valueLength = ByteBufUtil.utf8Bytes(value);
      byteBuf.writeInt(valueLength);
      ByteBufUtil.reserveAndWriteUtf8(byteBuf, value, valueLength);
    }

    return byteBuf;
  }

  public static Optional<InetAddress> inetAddress(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    if (inetAddressLength > 0) {
      byte[] inetAddressBytes = new byte[inetAddressLength];
      byteBuf.getBytes(offset, inetAddressBytes);

      try {
        return Optional.of(InetAddress.getByAddress(inetAddressBytes));
      } catch (UnknownHostException | IndexOutOfBoundsException e) {
        return Optional.empty();
      }
    }

    return Optional.empty();
  }

  public static String group(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, groupLength, StandardCharsets.UTF_8);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength;

    return byteBuf.getLong(offset);
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, accessTokenLength);
  }

  public static UUID connectionId(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + accessTokenLength;

    long mostSigBits = byteBuf.getLong(offset);
    long leastSigBits = byteBuf.getLong(offset + Long.BYTES);

    return new UUID(mostSigBits, leastSigBits);
  }

  public static short additionalFlags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + accessTokenLength;

    offset += CONNECTION_ID_LENGTH;

    return byteBuf.getShort(offset);
  }

  public static Tags tags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + accessTokenLength;

    offset += CONNECTION_ID_LENGTH;

    // Additional flags
    offset += ADDITIONAL_FLAGS_SIZE;

    List<Tag> tags = new ArrayList<>();
    while (offset < byteBuf.readableBytes()) {
      int keyLength = byteBuf.getInt(offset);
      offset += Integer.BYTES;

      String key = byteBuf.toString(offset, keyLength, StandardCharsets.UTF_8);
      offset += keyLength;

      int valueLength = byteBuf.getInt(offset);
      offset += Integer.BYTES;

      String value = byteBuf.toString(offset, valueLength, StandardCharsets.UTF_8);
      offset += valueLength;

      tags.add(Tag.of(key, value));
    }

    return Tags.of(tags);
  }
}
