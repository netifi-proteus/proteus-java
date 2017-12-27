package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

/** */
public class SharedSecretFlyweightTest {
  @Test
  public void testEncode() {
    Random rnd = ThreadLocalRandom.current();
    byte[] pk = new byte[32];
    rnd.nextBytes(pk);
    byte[] sharedSecret = new byte[16];
    rnd.nextBytes(sharedSecret);
    int token = rnd.nextInt();

    int length = SharedSecretFlyweight.computeLength();
    ByteBuf byteBuf = Unpooled.buffer(length);

    int encodedLength =
        SharedSecretFlyweight.encode(
            byteBuf, token, Unpooled.wrappedBuffer(pk), Unpooled.wrappedBuffer(sharedSecret), 0);

    Assert.assertEquals(length, encodedLength);

    byte[] pk1 = new byte[32];
    SharedSecretFlyweight.publicKey(byteBuf).getBytes(0, pk1);

    Assert.assertArrayEquals(pk, pk1);

    byte[] sharedSecret1 = new byte[16];
    SharedSecretFlyweight.sharedSecret(byteBuf).getBytes(0, sharedSecret1);

    Assert.assertArrayEquals(sharedSecret, sharedSecret1);

    int token1 = SharedSecretFlyweight.token(byteBuf);

    Assert.assertEquals(token, token1);
  }
}
