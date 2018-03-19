package io.netifi.proteus.admin;

import io.netifi.proteus.admin.tracing.AdminTraceService;
import java.net.InetSocketAddress;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class NetifiAdminTest {
  @Test
  public void testAdminTraceService() {
    NetifiAdmin admin =
        NetifiAdmin.builder()
            .socketAddress(InetSocketAddress.createUnresolved("172.16.1.9", 6001))
            .build();

    AdminTraceService traceService = admin.adminTraceService();

    traceService.streamDataJson().doOnNext(System.out::println).blockLast();
   // traceService.streamDataJson().take(10).doOnNext(System.out::println).blockLast();
  }
}
