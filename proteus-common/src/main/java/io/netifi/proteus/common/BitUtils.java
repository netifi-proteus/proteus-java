package io.netifi.proteus.common;

public class BitUtils {
  /**
   * Calculate the next power of 2, greater than or equal to x.
   *
   * <p>From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
   *
   * @param x Value to round up
   * @return The next power of 2 from x inclusive
   */
  public static int ceilingNextPowerOfTwo(final int x) {
    return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
  }
}
