/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.netifi.proteus.rsocket.transport;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
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
            BrokerAddressSelectors.TCP_ADDRESS, address -> transport);

    supplier.select();
    DuplexConnection block = supplier.get().connect().block();

    int i = supplier.activeConnections();

    Assert.assertEquals(1, i);

    onClose.onComplete();
    block.onClose().block();

    i = supplier.activeConnections();

    Assert.assertEquals(0, i);
  }
}
