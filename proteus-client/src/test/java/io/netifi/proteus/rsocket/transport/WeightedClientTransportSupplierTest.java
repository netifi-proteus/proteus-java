package io.netifi.proteus.rsocket.transport;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.net.InetSocketAddress;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class WeightedClientTransportSupplierTest {
  @Test
  public void testShouldDecrementActiveCountOnComplete() {

    MonoProcessor<Void> onClose = MonoProcessor.create();
    DuplexConnection duplexConnection = Mockito.mock(DuplexConnection.class);
    Mockito.when(duplexConnection.onClose()).thenReturn(onClose);

    ClientTransport transport = Mockito.mock(ClientTransport.class);
    Mockito.when(transport.connect()).thenReturn(Mono.just(duplexConnection));

    WeightedClientTransportSupplier supplier =
        new WeightedClientTransportSupplier(
            InetSocketAddress.createUnresolved("localhost", 8081), address -> transport);

    supplier.activate();
    DuplexConnection block = supplier.get().connect().block();

    int i = supplier.activeConnections();

    Assert.assertEquals(1, i);

    onClose.onComplete();
    block.onClose().block();

    i = supplier.activeConnections();

    Assert.assertEquals(0, i);
  }
}
