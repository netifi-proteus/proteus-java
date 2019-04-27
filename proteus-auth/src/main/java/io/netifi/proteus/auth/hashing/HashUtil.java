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
package io.netifi.proteus.auth.hashing;

import io.netifi.proteus.common.ByteUtil;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class HashUtil {

  private HashUtil() {}

  public static byte[] hashMD5(String s) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      return md.digest(bytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static long[] hashM5ToLongs(String s) {
    byte[] bytes = hashMD5(s);
    long first = ByteUtil.toLong(bytes, 0);
    long second = ByteUtil.toLong(bytes, 8);
    return new long[] {first, second};
  }

  public static String hashM5toString(String s) {
    byte[] bytes = hashMD5(s);
    return ByteUtil.printHexBinary(bytes).toUpperCase();
  }
}
