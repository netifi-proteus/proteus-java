package io.netifi.proteus.admin.frames;

import io.netifi.proteus.frames.admin.AdminRouterNodeInfoEventType;
import io.netifi.proteus.frames.admin.AdminRouterNodeInfoResultFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class AdminRouterNodeInfoResultFlyweightTest {
  @Test
  public void testEncodeAndDecode() {
    String routerId = UUID.randomUUID().toString();
    String routerAddress = "127.0.0.1";
    int routerPort = 8001;
    String clusterAddress = "127.0.0.1";
    int clusterPort = 7001;
    String adminAddress = "127.0.0.1";
    int adminPort = 6001;
    int length =
        AdminRouterNodeInfoResultFlyweight.computeLength(
            routerId, routerAddress, clusterAddress, adminAddress);
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
    AdminRouterNodeInfoResultFlyweight.encode(
        byteBuf,
        AdminRouterNodeInfoEventType.JOIN,
        routerId,
        routerAddress,
        routerPort,
        clusterAddress,
        clusterPort,
        adminAddress,
        adminPort);

    Assert.assertEquals(routerId, AdminRouterNodeInfoResultFlyweight.routerId(byteBuf));
    Assert.assertEquals(routerAddress, AdminRouterNodeInfoResultFlyweight.routerAddress(byteBuf));
    Assert.assertEquals(routerPort, AdminRouterNodeInfoResultFlyweight.routerPort(byteBuf));
    Assert.assertEquals(clusterAddress, AdminRouterNodeInfoResultFlyweight.clusterAddress(byteBuf));
    Assert.assertEquals(clusterPort, AdminRouterNodeInfoResultFlyweight.clusterPort(byteBuf));
    Assert.assertEquals(adminAddress, AdminRouterNodeInfoResultFlyweight.adminAddress(byteBuf));
    Assert.assertEquals(adminPort, AdminRouterNodeInfoResultFlyweight.adminPort(byteBuf));
  }
}
