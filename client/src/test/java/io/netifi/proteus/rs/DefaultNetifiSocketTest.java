package io.netifi.proteus.rs;

import io.netifi.proteus.balancer.LoadBalancedRSocketSupplier;
import io.netifi.proteus.frames.RouteDestinationFlyweight;
import io.netifi.proteus.frames.RoutingFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultNetifiSocketTest {
  private static final TimebasedIdGenerator idGenerator = new TimebasedIdGenerator(1);

  @Test
  public void testRequestResponse() {
    byte[] token = new byte[20];
    ThreadLocalRandom.current().nextBytes(token);
    MonoProcessor<Void> onClose = MonoProcessor.create();
    WeightedReconnectingRSocket mock = Mockito.mock(WeightedReconnectingRSocket.class);
    Mockito.when(mock.onClose()).thenReturn(onClose);
    Mockito.when(mock.getCurrentSessionCounter()).thenReturn(Mono.just(new AtomicLong()));
    Mockito.when(mock.getCurrentSessionToken()).thenReturn(Mono.just(token));

    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.empty());

    Mockito.when(mock.requestResponse(Mockito.any(Payload.class)))
        .then(
            invocation -> {
              Payload payload = (Payload) invocation.getArguments()[0];

              ByteBuf route = RoutingFlyweight.route(payload.sliceMetadata());
              long accountId = RouteDestinationFlyweight.accountId(route);
              Assert.assertEquals(Long.MAX_VALUE, accountId);
              return Mono.just(ByteBufPayload.create("here's the payload"));
            });

    DefaultNetifiSocket netifiSocket =
        new DefaultNetifiSocket(
            supplier,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            "fromDest",
            "toDest",
            "toGroup",
            token,
            false,
            idGenerator);

    byte[] metadata = new byte[1024];
    ThreadLocalRandom.current().nextBytes(metadata);

    netifiSocket
        .requestResponse(ByteBufPayload.create("hi".getBytes(), metadata))
        .doOnError(Throwable::printStackTrace)
        .block();
  }

  @Test
  public void testFireForget() {
    byte[] token = new byte[20];
    ThreadLocalRandom.current().nextBytes(token);
    MonoProcessor<Void> onClose = MonoProcessor.create();
    WeightedReconnectingRSocket mock = Mockito.mock(WeightedReconnectingRSocket.class);
    Mockito.when(mock.onClose()).thenReturn(onClose);
    Mockito.when(mock.getCurrentSessionCounter()).thenReturn(Mono.just(new AtomicLong()));
    Mockito.when(mock.getCurrentSessionToken()).thenReturn(Mono.just(token));

    Mockito.when(mock.fireAndForget(Mockito.any(Payload.class)))
        .then(
            invocation -> {
              Payload payload = (Payload) invocation.getArguments()[0];

              ByteBuf route = RoutingFlyweight.route(payload.sliceMetadata());
              long accountId = RouteDestinationFlyweight.accountId(route);
              Assert.assertEquals(Long.MAX_VALUE, accountId);
              return Mono.empty();
            });

    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.empty());

    DefaultNetifiSocket netifiSocket =
        new DefaultNetifiSocket(
            supplier,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            "fromDest",
            "toDest",
            "toGroup",
            token,
            false,
            idGenerator);

    byte[] metadata = new byte[1024];
    ThreadLocalRandom.current().nextBytes(metadata);

    netifiSocket
        .fireAndForget(ByteBufPayload.create("hi".getBytes(), metadata))
        .doOnError(Throwable::printStackTrace)
        .block();
  }

  @Test
  public void testRequestStream() {
    byte[] token = new byte[20];
    ThreadLocalRandom.current().nextBytes(token);
    MonoProcessor<Void> onClose = MonoProcessor.create();
    WeightedReconnectingRSocket mock = Mockito.mock(WeightedReconnectingRSocket.class);
    Mockito.when(mock.onClose()).thenReturn(onClose);
    Mockito.when(mock.getCurrentSessionCounter()).thenReturn(Mono.just(new AtomicLong()));
    Mockito.when(mock.getCurrentSessionToken()).thenReturn(Mono.just(token));

    Mockito.when(mock.requestStream(Mockito.any(Payload.class)))
        .then(
            invocation -> {
              Payload payload = (Payload) invocation.getArguments()[0];

              ByteBuf route = RoutingFlyweight.route(payload.sliceMetadata());
              long accountId = RouteDestinationFlyweight.accountId(route);
              Assert.assertEquals(Long.MAX_VALUE, accountId);
              return Flux.range(1, 100).map(i -> ByteBufPayload.create("here's the payload " + i));
            });

    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.empty());

    DefaultNetifiSocket netifiSocket =
        new DefaultNetifiSocket(
            supplier,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            "fromDest",
            "toDest",
            "toGroup",
            token,
            false,
            idGenerator);

    byte[] metadata = new byte[1024];
    ThreadLocalRandom.current().nextBytes(metadata);

    netifiSocket
        .requestStream(ByteBufPayload.create("hi".getBytes(), metadata))
        .doOnError(Throwable::printStackTrace)
        .blockLast();
  }

  @Test
  public void testRequestChannel() {
    byte[] token = new byte[20];
    ThreadLocalRandom.current().nextBytes(token);
    MonoProcessor<Void> onClose = MonoProcessor.create();
    WeightedReconnectingRSocket mock = Mockito.mock(WeightedReconnectingRSocket.class);
    Mockito.when(mock.onClose()).thenReturn(onClose);
    Mockito.when(mock.getCurrentSessionCounter()).thenReturn(Mono.just(new AtomicLong()));
    Mockito.when(mock.getCurrentSessionToken()).thenReturn(Mono.just(token));

    Mockito.when(mock.requestChannel(Mockito.any(Publisher.class)))
        .then(
            invocation -> {
              Publisher<Payload> payloads = (Publisher) invocation.getArguments()[0];

              return Flux.from(payloads)
                  .doOnNext(
                      payload -> {
                        ByteBuf route = RoutingFlyweight.route(payload.sliceMetadata());
                        long accountId = RouteDestinationFlyweight.accountId(route);
                        Assert.assertEquals(Long.MAX_VALUE, accountId);
                      })
                  .flatMap(
                      payload ->
                          Flux.range(1, 100)
                              .map(i -> ByteBufPayload.create("here's the payload " + i)));
            });

    LoadBalancedRSocketSupplier supplier = Mockito.mock(LoadBalancedRSocketSupplier.class);
    Mockito.when(supplier.get()).thenReturn(mock);
    Mockito.when(supplier.onClose()).thenReturn(Mono.empty());

    DefaultNetifiSocket netifiSocket =
        new DefaultNetifiSocket(
            supplier,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            "fromDest",
            "toDest",
            "toGroup",
            token,
            false,
            idGenerator);

    byte[] metadata = new byte[1024];
    ThreadLocalRandom.current().nextBytes(metadata);

    netifiSocket
        .requestChannel(Mono.just(ByteBufPayload.create("hi".getBytes(), metadata)))
        .doOnError(Throwable::printStackTrace)
        .blockLast();
  }
}
