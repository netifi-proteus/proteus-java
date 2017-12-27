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
            .socketAddress(InetSocketAddress.createUnresolved("127.0.0.1", 6001))
            .build();

    AdminTraceService traceService = admin.adminTraceService();

    traceService.streamDataJson().take(10).doOnNext(System.out::println).blockLast();
  }
}
