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

import java.security.SecureRandom;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class AccessTokenHasherTest {
  @Test
  public void testHashing() throws Exception {
    byte[] salt = new byte[20];
    byte[] accessToken = new byte[256];

    SecureRandom random = new SecureRandom();
    random.nextBytes(salt);
    random.nextBytes(accessToken);

    AccessTokenHasher accessTokenHasher = AccessTokenHasher.defaultInstance();
    byte[] hash = accessTokenHasher.hash(salt, accessToken);

    Assert.assertNotNull(hash);

    System.out.println(Base64.getEncoder().encodeToString(hash));
  }

  @Test
  public void testVerify() throws Exception {
    byte[] salt = new byte[20];
    byte[] accessToken = new byte[256];

    SecureRandom random = new SecureRandom();
    random.nextBytes(salt);
    random.nextBytes(accessToken);

    AccessTokenHasher accessTokenHasher = AccessTokenHasher.defaultInstance();
    byte[] hash = accessTokenHasher.hash(salt, accessToken);

    Assert.assertNotNull(hash);

    boolean verify = accessTokenHasher.verify(salt, accessToken, hash);
    Assert.assertTrue(verify);
  }

  @Test
  public void testGenerateHash() {
    byte[] salt = new byte[20];
    byte[] accessToken = new byte[20];

    SecureRandom random = new SecureRandom();
    random.nextBytes(salt);
    random.nextBytes(accessToken);

    AccessTokenHasher accessTokenHasher = AccessTokenHasher.defaultInstance();
    byte[] hash = accessTokenHasher.hash(salt, accessToken);

    Assert.assertNotNull(hash);

    System.out.println("hash: " + Base64.getEncoder().encodeToString(hash));
    System.out.println("salt: " + Base64.getEncoder().encodeToString(salt));
    System.out.println("accessToken: " + Base64.getEncoder().encodeToString(accessToken));
  }
}
