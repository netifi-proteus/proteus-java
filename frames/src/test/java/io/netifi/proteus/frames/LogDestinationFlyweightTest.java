package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/** */
public class LogDestinationFlyweightTest {
  @Test
  public void testEncodeDestination() {
    int length = LogDestinationFlyweight.computeLength(RouteType.STREAM_ID_ROUTE, 0);
    byte[] bytes = new byte[length];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
    int seqId = 100;
    int accountId = 1;
    int destinationId = 2;
    LogDestinationFlyweight.encodeRouteByDestination(byteBuf, 100, RouteType.STREAM_ID_ROUTE, 1, 2);

    Assert.assertEquals(seqId, LogDestinationFlyweight.seqId(byteBuf));
    Assert.assertEquals(accountId, LogDestinationFlyweight.accountId(byteBuf));
    Assert.assertEquals(destinationId, LogDestinationFlyweight.destinationId(byteBuf));
    Assert.assertEquals(RouteType.STREAM_ID_ROUTE, LogDestinationFlyweight.routeType(byteBuf));
  }
}
