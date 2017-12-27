package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

/** */
public class RouteDestinationFlyweightTest {
  @Test
  public void testComputeLengthWithDestination() {
    int expected = 27;
    int length =
        RouteDestinationFlyweight.computeLength(RouteType.STREAM_ID_ROUTE, "destination", "group");
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testComputeLengthWithGroups() {
    int expected = 15;
    int length = RouteDestinationFlyweight.computeLength(RouteType.STREAM_GROUP_ROUTE, "group");
    Assert.assertEquals(expected, length);
  }

  @Test
  public void testEncodeRouteByDestination() {
    Random rnd = ThreadLocalRandom.current();
    String group = "group";
    String destination = "dest";
    int length =
        RouteDestinationFlyweight.computeLength(RouteType.STREAM_ID_ROUTE, destination, group);
    RouteType routeType = RouteType.STREAM_ID_ROUTE;
    long accountId = Math.abs(rnd.nextLong());

    ByteBuf byteBuf = Unpooled.buffer(length);

    RouteDestinationFlyweight.encodeRouteByDestination(
        byteBuf, routeType, accountId, destination, group);

    RouteType routeType1 = RouteDestinationFlyweight.routeType(byteBuf);
    Assert.assertEquals(routeType, routeType1);

    long accountId1 = RouteDestinationFlyweight.accountId(byteBuf);
    Assert.assertEquals(accountId, accountId1);

    CharSequence destination1 = RouteDestinationFlyweight.destination(byteBuf);
    Assert.assertEquals(destination, destination1);

    Assert.assertEquals(group, RouteDestinationFlyweight.group(byteBuf));
  }

  @Test
  public void testEncodeRouteByGroup() {
    Random rnd = ThreadLocalRandom.current();
    String group = "group";
    int length = RouteDestinationFlyweight.computeLength(RouteType.STREAM_GROUP_ROUTE, group);
    RouteType routeType = RouteType.STREAM_GROUP_ROUTE;
    long accountId = Math.abs(rnd.nextLong());

    byte[] bytes = new byte[length];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

    RouteDestinationFlyweight.encodeRouteByGroup(byteBuf, routeType, accountId, group);

    RouteType routeType1 = RouteDestinationFlyweight.routeType(byteBuf);
    Assert.assertEquals(routeType, routeType1);

    long accountId1 = RouteDestinationFlyweight.accountId(byteBuf);
    Assert.assertEquals(accountId, accountId1);

    CharSequence group1 = RouteDestinationFlyweight.group(byteBuf);
    Assert.assertEquals(group, group1);
  }
}
