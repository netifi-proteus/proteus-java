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
