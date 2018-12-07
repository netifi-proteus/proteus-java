package io.netifi.proteus.frames;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DestinationSetupFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator,
      InetAddress inetAddress,
      CharSequence group,
      long accessKey,
      byte[] accessToken,
      Tags tags) {
    return encode(
        allocator, inetAddress, group, accessKey, Unpooled.wrappedBuffer(accessToken), tags);
  }

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      InetAddress inetAddress,
      CharSequence group,
      long accessKey,
      ByteBuf accessToken,
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

  public static Tags tags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int inetAddressLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + inetAddressLength;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength + Long.BYTES;

    int accessTokenLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + accessTokenLength;

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
