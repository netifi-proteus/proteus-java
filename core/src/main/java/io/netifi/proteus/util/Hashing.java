/*
 *  Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netifi.proteus.util;

/** Hashing functions for applying to integers. */
public class Hashing {
  private static final int FNV_32_PRIME = 0x01000193;
  private static final int FNV_32_SEED = 0x811c9dc5;

  // FNV-1a hash
  public static int hash(final String value) {
    int rv = FNV_32_SEED;
    final int len = value.length();
    for (int i = 0; i < len; i++) {
      rv ^= value.charAt(i);
      rv *= FNV_32_PRIME;
    }
    return rv;
  }

  /**
   * Generate a hash for a long value.
   *
   * @param value to be hashed.
   * @param mask mask to be applied that must be a power of 2 - 1.
   * @return the hash of the value.
   */
  public static int hash(final long value, final int mask) {
    long hash = value * 31;
    hash = (int) hash ^ (int) (hash >>> 32);

    return (int) hash & mask;
  }

  /**
   * Combined two 32 bit keys into a 64-bit compound.
   *
   * @param keyPartA to make the upper bits
   * @param keyPartB to make the lower bits.
   * @return the compound key
   */
  public static long compoundKey(final int keyPartA, final int keyPartB) {
    return ((long) keyPartA << 32) | (keyPartB & 0xFFFFFFFFL);
  }
}
