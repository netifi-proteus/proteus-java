package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/** Created by robertroeser on 8/12/17. */
public class DestinationAvailResultTest {
  @Test
  public void testComputeLengthWithToken() {
    int expected = 18;
    String destination = "dest";
    int length = DestinationAvailResult.computeLength(destination);
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testComputeLengthWithoutToken() {
    int expected = 18;
    String destination = "dest";
    int length = DestinationAvailResult.computeLength(destination);
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testEncode() {
    String destination = "dest.test.2";
    int length = DestinationAvailResult.computeLength(destination);
    boolean found = false;
    ByteBuf byteBuf = Unpooled.buffer(length);
    DestinationAvailResult.encode(byteBuf, destination, found, 0);
    Assert.assertEquals(found, DestinationAvailResult.found(byteBuf));
    Assert.assertEquals(destination, DestinationAvailResult.destination(byteBuf));
  }
}
