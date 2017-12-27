package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;

/** Miscellanous bit utils */
public class BitUtil {
  /** Size of a byte in bytes */
  public static final int SIZE_OF_BYTE = 1;

  /** Size of a boolean in bytes */
  public static final int SIZE_OF_BOOLEAN = 1;

  /** Size of a char in bytes */
  public static final int SIZE_OF_CHAR = 2;

  /** Size of a short in bytes */
  public static final int SIZE_OF_SHORT = 2;

  /** Size of an int in bytes */
  public static final int SIZE_OF_INT = 4;

  /** Size of a a float in bytes */
  public static final int SIZE_OF_FLOAT = 4;

  /** Size of a long in bytes */
  public static final int SIZE_OF_LONG = 8;

  /** Size of a double in bytes */
  public static final int SIZE_OF_DOUBLE = 8;

  /** Empty Long Array */
  public static final long[] EMPTY = new long[0];

  /** Empty ByteBuf Array */
  public static final ByteBuf[] EMPTY_BYTE_BUFS = new ByteBuf[0];

  public static int toUnsignedInt(byte x) {
    return ((int) x) & 0xff;
  }
}
