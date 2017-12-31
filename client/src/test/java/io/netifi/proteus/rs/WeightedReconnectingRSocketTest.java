package io.netifi.proteus.rs;

import io.netifi.proteus.balancer.transport.ClientTransportSupplierFactory;
import io.netifi.proteus.discovery.DestinationNameFactory;
import io.netifi.proteus.stats.FrugalQuantile;
import io.rsocket.RSocket;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class WeightedReconnectingRSocketTest {
  @Test
  public void testShouldWaitForSocketWhenNotPresent() {
    byte[] accessTokenBytes = new byte[20];
    ThreadLocalRandom.current().nextBytes(accessTokenBytes);

    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            Mockito.mock(RSocket.class),
            Mockito.mock(DestinationNameFactory.class),
            Mockito.mock(Function.class),
            () -> true,
            Mockito.mock(ClientTransportSupplierFactory.class),
            false,
            0,
            0,
            0,
            0,
            accessTokenBytes,
            new FrugalQuantile(0.2),
            new FrugalQuantile(0.6),
            1);

    rSocket.resetMono();

    StepVerifier.create(rSocket.getRSocket())
        .expectNextCount(0)
        .thenCancel()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  public void testShouldSetRSocketAndReturnSocket() {
    byte[] accessTokenBytes = new byte[20];
    ThreadLocalRandom.current().nextBytes(accessTokenBytes);

    WeightedReconnectingRSocket rSocket =
        Mockito.spy(
            new WeightedReconnectingRSocket(
                Mockito.mock(RSocket.class),
                Mockito.mock(DestinationNameFactory.class),
                Mockito.mock(Function.class),
                () -> true,
                Mockito.mock(ClientTransportSupplierFactory.class),
                false,
                0,
                0,
                0,
                0,
                accessTokenBytes,
                new FrugalQuantile(0.2),
                new FrugalQuantile(0.6),
                1));

    rSocket.resetMono();

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.onClose()).thenReturn(Mono.never());

    rSocket.setRSocket(mock);

    StepVerifier.create(rSocket.getRSocket()).expectNextCount(1).verifyComplete();

    Mockito.verify(rSocket, Mockito.times(1)).resetStatistics();

    AtomicLong counter = rSocket.getCurrentSessionCounter().block();
    byte[] token = rSocket.getCurrentSessionToken().block();

    RSocket mock2 = Mockito.mock(RSocket.class);
    Mockito.when(mock2.onClose()).thenReturn(Mono.never());

    rSocket.setRSocket(mock2);

    AtomicLong resetCount = rSocket.getCurrentSessionCounter().block();
    byte[] resetToken = rSocket.getCurrentSessionToken().block();

    Assert.assertNotSame(counter, resetCount);
    Assert.assertNotSame(token, resetToken);
  }

  @Test
  public void testShouldEmitNewRSocketAfterSubscribing() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    byte[] accessTokenBytes = new byte[20];
    ThreadLocalRandom.current().nextBytes(accessTokenBytes);

    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            Mockito.mock(RSocket.class),
            Mockito.mock(DestinationNameFactory.class),
            Mockito.mock(Function.class),
            () -> true,
            Mockito.mock(ClientTransportSupplierFactory.class),
            false,
            0,
            0,
            0,
            0,
            accessTokenBytes,
            new FrugalQuantile(0.2),
            new FrugalQuantile(0.6),
            1);

    rSocket.resetMono();

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.onClose()).thenReturn(Mono.never());

    rSocket.getRSocket().subscribe(r -> latch.countDown());

    rSocket.setRSocket(mock);

    latch.await();
  }

  @Test
  public void testShouldWaitAfterRSocketCloses() {
    byte[] accessTokenBytes = new byte[20];
    ThreadLocalRandom.current().nextBytes(accessTokenBytes);

    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            Mockito.mock(RSocket.class),
            Mockito.mock(DestinationNameFactory.class),
            Mockito.mock(Function.class),
            () -> true,
            Mockito.mock(ClientTransportSupplierFactory.class),
            false,
            0,
            0,
            0,
            0,
            accessTokenBytes,
            new FrugalQuantile(0.2),
            new FrugalQuantile(0.6),
            1);

    rSocket.resetMono();

    MonoProcessor<Void> processor = MonoProcessor.create();
    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.onClose()).thenReturn(processor);

    rSocket.setRSocket(mock);

    processor.onComplete();

    StepVerifier.create(rSocket.getRSocket())
        .expectNextCount(0)
        .thenCancel()
        .verify(Duration.ofSeconds(1));
  }
}
