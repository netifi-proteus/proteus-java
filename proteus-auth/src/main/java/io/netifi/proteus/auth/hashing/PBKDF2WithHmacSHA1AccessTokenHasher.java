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

import java.security.spec.KeySpec;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * {@link AccessTokenHasher} implementation that uses the PBKDF2WithHmacSHA1 algorithm. It uses 4096
 * and produces a 256-bit hash
 */
class PBKDF2WithHmacSHA1AccessTokenHasher implements AccessTokenHasher {
  static final PBKDF2WithHmacSHA1AccessTokenHasher INSTANCE =
      new PBKDF2WithHmacSHA1AccessTokenHasher();

  @Override
  public byte[] hash(byte[] salt, byte[] accessToken) {
    try {
      SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
      Base64.Encoder encoder = Base64.getEncoder();
      char[] chars = encoder.encodeToString(accessToken).toCharArray();
      KeySpec keySpec = new PBEKeySpec(chars, salt, 4096, 256);
      SecretKey secretKey = factory.generateSecret(keySpec);
      return secretKey.getEncoded();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
