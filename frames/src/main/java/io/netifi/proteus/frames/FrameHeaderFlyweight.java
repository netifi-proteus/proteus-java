package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** */
public class FrameHeaderFlyweight {
  // Protocol Version
  public static final int MAJOR_VERSION = 0;
  public static final int MINOR_VERSION = 1;
  // Flag Fields masks
  static final int USER_DATA_PRESENT = 0b1000_0000;
  static final int METADATA_PRESENT = 0b0100_0000;
  static final int ENCRYPTED = 0b0010_0000;
  static final int BROADCAST = 0b0001_0000;
  static final int API_CALL = 0b0000_1000;
  static final int TOKEN = 0b0000_0100;
  // Frame Header field masks
  private static final int FRAME_TYPE_MASK = 0b0111_1111_0000_0000_0000_0000_0000_0000;
  private static final int FLAGS_MASK = 0b0000_0000_1111_1111_0000_0000_0000_0000;
  private static final int MAJOR_VERSION_MASK = 0b0000_0000_0000_0000_1111_1111_0000_0000;
  private static final int MINOR_VERSION_MASK = 0b0000_0000_0000_0000_0000_0000_1111_1111;

  private FrameHeaderFlyweight() {}

  public static int computeFrameHeaderLength() {
    return BitUtil.SIZE_OF_INT + BitUtil.SIZE_OF_LONG;
  }

  public static int encodeFrameHeader(
      final ByteBuf byteBuf, final FrameType frameType, final int flags, long seqId) {
    return encodeFrameHeader(byteBuf, frameType, flags, MAJOR_VERSION, MINOR_VERSION, seqId);
  }

  public static int encodeFrameHeader(
      final ByteBuf byteBuf,
      final FrameType frameType,
      final int flags,
      final int majorVersion,
      final int minorVersion,
      long seqId) {

    int header =
        (frameType.getEncodedType() << 24) | (flags << 16) | (majorVersion << 8) | (minorVersion);

    byteBuf.setInt(0, header);
    byteBuf.setLong(BitUtil.SIZE_OF_INT, seqId);

    int offset = computeFrameHeaderLength();

    byteBuf.writerIndex(offset);

    return offset;
  }

  public static FrameType frameType(ByteBuf byteBuf) {
    int frameType = (byteBuf.getInt(0) & FRAME_TYPE_MASK) >>> 24;
    return FrameType.from(frameType);
  }

  public static int flags(ByteBuf byteBuf) {
    int flags = (byteBuf.getInt(0) & FLAGS_MASK) >>> 16;
    return flags;
  }

  public static int majorVersion(ByteBuf byteBuf) {
    int flags = (byteBuf.getInt(0) & MAJOR_VERSION_MASK) >>> 8;
    return flags;
  }

  public static int minorVersion(ByteBuf byteBuf) {
    int flags = byteBuf.getInt(0) & MINOR_VERSION_MASK;
    return flags;
  }

  public static int encodeFlags(
      boolean userData, boolean metadata, boolean encrypted, boolean apiCall, boolean token) {
    int flags =
        (userData ? USER_DATA_PRESENT : 0)
            | (metadata ? METADATA_PRESENT : 0)
            | (encrypted ? ENCRYPTED : 0)
            | (apiCall ? API_CALL : 0)
            | (token ? TOKEN : 0);

    return flags;
  }

  public static boolean hasData(ByteBuf byteBuf) {
    return (flags(byteBuf) & USER_DATA_PRESENT) == USER_DATA_PRESENT;
  }

  public static boolean hasMetadata(ByteBuf byteBuf) {
    return (flags(byteBuf) & METADATA_PRESENT) == METADATA_PRESENT;
  }

  public static boolean encrypted(ByteBuf byteBuf) {
    return (flags(byteBuf) & ENCRYPTED) != 0;
  }

  public static long seqId(ByteBuf byteBuf) {
    return byteBuf.getLong(BitUtil.SIZE_OF_INT);
  }

  public static boolean broadcast(ByteBuf byteBuf) {
    return (flags(byteBuf) & BROADCAST) != 0;
  }

  public static boolean apiCall(ByteBuf byteBuf) {
    return (flags(byteBuf) & API_CALL) != 0;
  }

  public static boolean token(ByteBuf byteBuf) {
    return (flags(byteBuf) & TOKEN) == TOKEN;
  }
}
