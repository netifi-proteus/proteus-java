package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

/** */
public class RouterSetupFlyweightTest {
  @Test
  public void testEncode() {
    Random rnd = ThreadLocalRandom.current();
    byte[] token = new byte[256];
    rnd.nextBytes(token);
    int clusterId = Math.abs(rnd.nextInt());
    int routerId = Math.abs(rnd.nextInt());
    int length = RouterSetupFlyweight.computeLength(token.length);
    ByteBuf byteBuf = Unpooled.buffer(length);
    int offset =
        RouterSetupFlyweight.encode(byteBuf, clusterId, routerId, Unpooled.wrappedBuffer(token), 0);
    Assert.assertEquals(length, offset);

    int clusterId1 = RouterSetupFlyweight.clusterId(byteBuf);
    Assert.assertEquals(clusterId, clusterId1);

    int routerId1 = RouterSetupFlyweight.routerId(byteBuf);
    Assert.assertEquals(routerId, routerId1);

    byte[] token1 = new byte[token.length];
    RouterSetupFlyweight.authToken(byteBuf).getBytes(0, token1);
    Assert.assertArrayEquals(token, token1);
  }
}
