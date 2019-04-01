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

import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

public class PBKDF2WithHmacSHA1AccessTokenHasherTest {
  @Test
  public void testHash() {
    PBKDF2WithHmacSHA1AccessTokenHasher hasher = new PBKDF2WithHmacSHA1AccessTokenHasher();
    byte[] salt = new byte[20];
    byte[] accessToken = new byte[20];
    ThreadLocalRandom.current().nextBytes(salt);
    ThreadLocalRandom.current().nextBytes(accessToken);
    byte[] hash = hasher.hash(salt, accessToken);

    Assert.assertNotNull(hash);
    Assert.assertTrue(hash.length > 0);
  }
}
