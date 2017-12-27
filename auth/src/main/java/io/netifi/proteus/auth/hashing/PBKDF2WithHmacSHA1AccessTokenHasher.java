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
