package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

/** */
public class RequestSharedSecretFlyweightTest {
  @Test
  public void testEncode() {
    Random rnd = ThreadLocalRandom.current();
    int token = rnd.nextInt();
    byte[] pk = new byte[32];
    rnd.nextBytes(pk);

    int length = RequestSharedSecretFlyweight.computeLength();
    ByteBuf byteBuf = Unpooled.buffer(length);
    int offset = RequestSharedSecretFlyweight.encode(byteBuf, token, Unpooled.wrappedBuffer(pk), 0);

    Assert.assertEquals(length, offset);

    byte[] pk1 = new byte[32];
    RequestSharedSecretFlyweight.publicKey(byteBuf).getBytes(0, pk1);

    Assert.assertArrayEquals(pk, pk1);

    int token1 = RequestSharedSecretFlyweight.token(byteBuf);
    Assert.assertEquals(token, token1);
  }
}
