package io.netifi.proteus.rsocket;

import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.rsocket.rpc.stats.FrugalQuantile;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

public class WeightedReconnectingRSocketTest {

  @Test
  public void testShouldWaitForSocketWhenNotPresent() {
    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            Mockito.mock(RSocket.class),
            Mockito.mock(Supplier.class),
            () -> true,
            Mockito.mock(Supplier.class),
            false,
            0,
            0,
            0,
            0,
            Unpooled.EMPTY_BUFFER,
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
    WeightedReconnectingRSocket rSocket =
        Mockito.spy(
            new WeightedReconnectingRSocket(
                Mockito.mock(RSocket.class),
                Mockito.mock(Supplier.class),
                () -> true,
                Mockito.mock(Supplier.class),
                false,
                0,
                0,
                0,
                0,
                Unpooled.EMPTY_BUFFER,
                new FrugalQuantile(0.2),
                new FrugalQuantile(0.6),
                1));

    rSocket.resetMono();

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.onClose()).thenReturn(Mono.never());

    rSocket.setRSocket(mock);
    StepVerifier.create(rSocket.getRSocket())
        .expectNextMatches(Predicate.isEqual(mock))
        .verifyComplete();

    Mockito.verify(rSocket, Mockito.times(1)).resetStatistics();

    rSocket.resetMono();

    RSocket mock2 = Mockito.mock(RSocket.class);
    Mockito.when(mock2.onClose()).thenReturn(Mono.never());
    rSocket.setRSocket(mock2);

    StepVerifier.create(rSocket.getRSocket())
        .expectNextMatches(Predicate.isEqual(mock2))
        .verifyComplete();
  }

  @Test
  public void testShouldEmitNewRSocketAfterSubscribing() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            Mockito.mock(RSocket.class),
            Mockito.mock(Supplier.class),
            () -> true,
            Mockito.mock(Supplier.class),
            false,
            0,
            0,
            0,
            0,
            Unpooled.EMPTY_BUFFER,
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

    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            Mockito.mock(RSocket.class),
            Mockito.mock(Supplier.class),
            () -> true,
            Mockito.mock(Supplier.class),
            false,
            0,
            0,
            0,
            0,
            Unpooled.EMPTY_BUFFER,
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
