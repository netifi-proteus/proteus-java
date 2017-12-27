package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/** Created by robertroeser on 8/12/17. */
public class QueryDestinationAvailFlyweightTest {
  @Test
  public void testComputeLengthWithToken() {
    int expected = 40;
    int length = QueryDestinationAvailFlyweight.computeLength(true);
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testComputeLengthWithoutToken() {
    int expected = 36;
    int length = QueryDestinationAvailFlyweight.computeLength(false);
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testEncode() {
    int length = QueryDestinationAvailFlyweight.computeLength(true);
    ByteBuf byteBuf = Unpooled.buffer(length);
    int encodedLength = QueryDestinationAvailFlyweight.encode(byteBuf, true, 1, 2, 3, 4, 0);
    Assert.assertEquals(length, encodedLength);
    int token = QueryDestinationAvailFlyweight.token(byteBuf);
    Assert.assertEquals(1, token);
    long accessKey = QueryDestinationAvailFlyweight.accessKey(byteBuf);
    Assert.assertEquals(2, accessKey);
    long accountId = QueryDestinationAvailFlyweight.accountId(byteBuf);
    Assert.assertEquals(3, accountId);
    long destinationId = QueryDestinationAvailFlyweight.destinationId(byteBuf);
    Assert.assertEquals(4, destinationId);
  }
}
