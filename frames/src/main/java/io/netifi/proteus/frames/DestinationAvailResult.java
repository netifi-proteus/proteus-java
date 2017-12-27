package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

/** */
public class DestinationAvailResult {
  private static final int DESTINATION_LENGTH_SIZE = BitUtil.SIZE_OF_BYTE;
  private static int FOUND_SIZE = BitUtil.SIZE_OF_BYTE;

  public static int computeLength(String destination) {
    return FrameHeaderFlyweight.computeFrameHeaderLength()
        + DESTINATION_LENGTH_SIZE
        + FOUND_SIZE
        + destination.length();
  }

  public static int encode(ByteBuf byteBuf, String destination, boolean found, long seqId) {
    int destinationLength = destination.length();
    if (destinationLength > 255) {
      throw new IllegalArgumentException("destination is longer then 255 characters");
    }

    StringUtil.validateIsAscii(destination);

    int flags = FrameHeaderFlyweight.encodeFlags(false, false, false, false, false);
    int offset =
        FrameHeaderFlyweight.encodeFrameHeader(
            byteBuf, FrameType.DESTINATION_AVAIL_RESULT, flags, seqId);

    byteBuf.setByte(offset, found ? 1 : 0);
    offset += FOUND_SIZE;

    byteBuf.setByte(offset, destinationLength);
    offset += DESTINATION_LENGTH_SIZE;

    byteBuf.setCharSequence(offset, destination, StandardCharsets.US_ASCII);
    offset += destinationLength;

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static String destination(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength() + FOUND_SIZE;
    int length = BitUtil.toUnsignedInt(byteBuf.getByte(offset));
    offset += DESTINATION_LENGTH_SIZE;

    return (String) byteBuf.getCharSequence(offset, length, StandardCharsets.US_ASCII);
  }

  public static boolean found(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.computeFrameHeaderLength();
    return byteBuf.getByte(offset) == 1;
  }
}
