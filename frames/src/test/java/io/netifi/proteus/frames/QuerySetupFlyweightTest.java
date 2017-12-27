package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

/** */
public class QuerySetupFlyweightTest {
  @Test
  public void testEncode() {
    Random rnd = ThreadLocalRandom.current();
    byte[] accessToken = new byte[20];
    rnd.nextBytes(accessToken);
    long accessKey = rnd.nextLong();

    int length = QuerySetupFlyweight.computeLength();
    ByteBuf byteBuf = Unpooled.buffer(length);

    int encodedLength =
        QuerySetupFlyweight.encode(byteBuf, Unpooled.wrappedBuffer(accessToken), accessKey, 0);

    Assert.assertEquals(length, encodedLength);

    byte[] accessToken1 = new byte[accessToken.length];
    QuerySetupFlyweight.accessToken(byteBuf).getBytes(0, accessToken1);
    Assert.assertArrayEquals(accessToken, accessToken1);

    Assert.assertEquals(accessKey, QuerySetupFlyweight.accessKey(byteBuf));
  }
}
