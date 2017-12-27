package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

/** */
public class DestinationSetupFlyweightTest {
  @Test
  public void testComputeLengthEncrypted() {
    int expected = 83;
    int length = DestinationSetupFlyweight.computeLength(true, "dest", "group");
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testComputeLength() {
    int expected = 51;
    int length = DestinationSetupFlyweight.computeLength(false, "dest", "group");
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testEncodeWithEncryption() {
    Random rnd = ThreadLocalRandom.current();
    byte[] pk = new byte[32];
    rnd.nextBytes(pk);
    byte[] accessToken = new byte[20];
    rnd.nextBytes(accessToken);
    long accessKey = rnd.nextLong();
    String destination = "dest";
    String group = "group";

    int length = DestinationSetupFlyweight.computeLength(true, destination, group);
    ByteBuf byteBuf = Unpooled.buffer(length);

    int encodedLength =
        DestinationSetupFlyweight.encode(
            byteBuf,
            Unpooled.wrappedBuffer(pk),
            Unpooled.wrappedBuffer(accessToken),
            0,
            accessKey,
            destination,
            group);

    Assert.assertEquals(length, encodedLength);

    byte[] pk1 = new byte[pk.length];
    DestinationSetupFlyweight.publicKey(byteBuf).getBytes(0, pk1);
    Assert.assertArrayEquals(pk, pk1);

    byte[] accessToken1 = new byte[accessToken.length];
    DestinationSetupFlyweight.accessToken(byteBuf).getBytes(0, accessToken1);
    Assert.assertArrayEquals(accessToken, accessToken1);

    Assert.assertEquals(accessKey, DestinationSetupFlyweight.accessKey(byteBuf));
    Assert.assertEquals(destination, DestinationSetupFlyweight.destination(byteBuf));
    Assert.assertEquals(group, DestinationSetupFlyweight.group(byteBuf));
  }

  @Test
  public void testWithDestinationLongerThan128() {
    Random rnd = ThreadLocalRandom.current();
    byte[] pk = new byte[32];
    rnd.nextBytes(pk);
    byte[] accessToken = new byte[20];
    rnd.nextBytes(accessToken);
    long accessKey = rnd.nextLong();

    byte[] bytes = new byte[150];
    rnd.nextBytes(bytes);
    String destination = Base64.getEncoder().encodeToString(bytes);
    String group = "group";

    int length = DestinationSetupFlyweight.computeLength(true, destination, group);
    ByteBuf byteBuf = Unpooled.buffer(length);

    int encodedLength =
        DestinationSetupFlyweight.encode(
            byteBuf,
            Unpooled.wrappedBuffer(pk),
            Unpooled.wrappedBuffer(accessToken),
            0,
            accessKey,
            destination,
            group);

    Assert.assertEquals(length, encodedLength);

    byte[] pk1 = new byte[pk.length];
    DestinationSetupFlyweight.publicKey(byteBuf).getBytes(0, pk1);
    Assert.assertArrayEquals(pk, pk1);

    byte[] accessToken1 = new byte[accessToken.length];
    DestinationSetupFlyweight.accessToken(byteBuf).getBytes(0, accessToken1);
    Assert.assertArrayEquals(accessToken, accessToken1);

    Assert.assertEquals(accessKey, DestinationSetupFlyweight.accessKey(byteBuf));
    Assert.assertEquals(destination, DestinationSetupFlyweight.destination(byteBuf));
    Assert.assertEquals(group, DestinationSetupFlyweight.group(byteBuf));
  }
}
