package io.netifi.proteus.balancer;

import io.netifi.proteus.rs.SecureRSocket;
import io.netifi.proteus.rs.WeightedReconnectingRSocket;
import io.netifi.proteus.stats.Quantile;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.BiFunction;

public class LoadBalancedRSocketSupplierTest {
  @Test
  public void testGet() {
    BiFunction<Quantile, Quantile, WeightedReconnectingRSocket> socketSupplier =
        (quantile, quantile2) -> {
          WeightedReconnectingRSocket mock = Mockito.mock(WeightedReconnectingRSocket.class);
          Mockito.when(mock.predictedLatency()).thenReturn(1.0);
          Mockito.when(mock.pending()).thenReturn(2);
          Mockito.when(mock.availability()).thenReturn(1.0);

          return mock;
        };

    LoadBalancedRSocketSupplier supplier = new LoadBalancedRSocketSupplier(4, socketSupplier);

    SecureRSocket rSocket = supplier.get();

    Assert.assertNotNull(rSocket);
  }
}
