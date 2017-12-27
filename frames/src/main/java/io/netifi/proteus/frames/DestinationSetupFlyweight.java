package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;

/** */
public class DestinationSetupFlyweight {

  private static final int PUBLIC_KEY_SIZE = 32;
  private static final int ACCESS_TOKEN_SIZE = 20;
  private static final int ACCESS_KEY_SIZE = BitUtil.SIZE_OF_LONG;
  private static final int DESTINATION_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static final int GROUP_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;

  private DestinationSetupFlyweight() {}

  public static int computeLength(boolean encrypted, String destination, String group) {

    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + (encrypted ? PUBLIC_KEY_SIZE : 0)
        + ACCESS_TOKEN_SIZE
        + ACCESS_KEY_SIZE
        + DESTINATION_LENGTH_SIZE
        + GROUP_LENGTH_SIZE
        + destination.length()
        + group.length();
  }

  public static int encode(
      ByteBuf byteBuf,
      ByteBuf publicKey,
      ByteBuf accessToken,
      long seqId,
      long accessKey,
      String destination,
      String group) {

    int destinationLength = destination.length();
    int groupLength = group.length();

    if (destinationLength > 255) {
      throw new IllegalArgumentException("destination is longer then 255 characters");
    }

    if (groupLength > 255) {
      throw new IllegalArgumentException("group is longer then 255 characters");
    }

    if (accessToken.readableBytes() != ACCESS_TOKEN_SIZE) {
      throw new IllegalStateException(
          String.format(
              "invalid access token size: found %d, expected %d",
              accessToken.readableBytes(), ACCESS_TOKEN_SIZE));
    }

    boolean encrypted = publicKey != null && publicKey.readableBytes() > 0;

    if (encrypted && publicKey.readableBytes() != PUBLIC_KEY_SIZE) {
      throw new IllegalStateException(
          String.format(
              "invalid public key size: found %d, expected %d",
              publicKey.readableBytes(), PUBLIC_KEY_SIZE));
    }

    int offset =
        FrameHeaderFlyweight.encodeFrameHeader(
            byteBuf,
            FrameType.DESTINATION_SETUP,
            encrypted ? FrameHeaderFlyweight.ENCRYPTED : 0,
            seqId);

    if (encrypted) {
      byteBuf.setBytes(offset, publicKey);
      offset += PUBLIC_KEY_SIZE;
    }

    byteBuf.setBytes(offset, accessToken);
    offset += ACCESS_TOKEN_SIZE;

    byteBuf.setLong(offset, accessKey);
    offset += ACCESS_KEY_SIZE;

    byteBuf.setByte(offset, destinationLength);
    offset += DESTINATION_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, destination, StandardCharsets.US_ASCII);
    offset += destinationLength;

    byteBuf.setByte(offset, groupLength);
    offset += GROUP_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, group, StandardCharsets.US_ASCII);
    offset += groupLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static ByteBuf publicKey(ByteBuf byteBuf) {
    boolean encrypted = FrameHeaderFlyweight.encrypted(byteBuf);
    if (encrypted) {
      int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
      return byteBuf.slice(offset, PUBLIC_KEY_SIZE);
    } else {
      return Unpooled.EMPTY_BUFFER;
    }
  }

  public static ByteBuf accessToken(ByteBuf byteBuf) {
    int offset = calculatePublicKeyOffset(byteBuf);
    return byteBuf.slice(offset, ACCESS_TOKEN_SIZE);
  }

  public static long accessKey(ByteBuf byteBuf) {
    int offset = calculatePublicKeyOffset(byteBuf) + ACCESS_TOKEN_SIZE;
    return byteBuf.getLong(offset);
  }

  public static String destination(ByteBuf byteBuf) {
    int offset = calculatePublicKeyOffset(byteBuf) + ACCESS_TOKEN_SIZE + ACCESS_KEY_SIZE;
    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE;
    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  private static int calculatePublicKeyOffset(ByteBuf byteBuf) {
    boolean encrypted = FrameHeaderFlyweight.encrypted(byteBuf);
    return (encrypted ? PUBLIC_KEY_SIZE : 0) + FrameHeaderFlyweight.computeFrameHeaderLength();
  }

  public static String group(ByteBuf byteBuf) {
    int offset = calculatePublicKeyOffset(byteBuf) + ACCESS_TOKEN_SIZE + ACCESS_KEY_SIZE;
    int destinationLength = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE + destinationLength;

    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += GROUP_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }
}
