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
import org.junit.Assert;
import org.junit.Test;

public class HashUtilTest {
  @Test
  public void testHash() {
    String hashMe = "hello world";
    String s = HashUtil.hashM5toString(hashMe);
    byte[] bytes = HashUtil.hashMD5(hashMe);
    String s1 = ByteUtil.printHexBinary(bytes);
    Assert.assertEquals(s, s1);
  }

  @Test
  public void testToLongs() {
    String hashMe = "hello world";
    long[] longs = HashUtil.hashM5ToLongs(hashMe);
    System.out.println(longs[0]);
    System.out.println(longs[1]);
  }
}
