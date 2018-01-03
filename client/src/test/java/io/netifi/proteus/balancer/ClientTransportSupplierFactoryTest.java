package io.netifi.proteus.balancer;

import io.netifi.proteus.auth.SessionUtil;
import io.netifi.proteus.balancer.transport.ClientTransportSupplierFactory;
import io.netifi.proteus.balancer.transport.WeightedClientTransportSupplier;
import io.netifi.proteus.discovery.DiscoveryEvent;
import io.netifi.proteus.discovery.RouterInfoSocketAddressFactory;
import io.netifi.proteus.discovery.SocketAddressFactory;
import io.netifi.proteus.frames.DestinationSetupFlyweight;
import io.netifi.proteus.rs.SecureRSocket;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.tcp.TcpClient;

public class ClientTransportSupplierFactoryTest {
  private static final long accessKey = 3855261330795754807L;
  private static final String accessToken = "n9R9042eE1KaLtE56rbWjBIGymo=";

  @Test(expected = IllegalStateException.class)
  public void testShouldNotEmit() {
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(Flux::never, address -> null, 1, 1);
    SocketAddressFactory.from("localhost", 8080);

    factory.get().block(Duration.ofMillis(100));
  }

  @Test(timeout = 5000)
  public void testShouldEmitAfterFluxEmits() throws Exception {
    UnicastProcessor<DiscoveryEvent> events = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(() -> events, address -> null, 1, 1);

    factory.get().doOnSuccess(s -> latch.countDown()).subscribe();

    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)));

    latch.await();
  }

  @Test(timeout = 5000, expected = IllegalStateException.class)
  public void testShouldEmitThenTimeout() throws Exception {
    UnicastProcessor<DiscoveryEvent> events = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(() -> events, address -> null, 1, 10);

    factory.get().doOnSuccess(s -> latch.countDown()).subscribe();
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)));
    latch.await();
    events.onNext(DiscoveryEvent.remove(InetSocketAddress.createUnresolved("localhost", 8080)));
    factory.get().block(Duration.ofMillis(100));
  }

  @Test(timeout = 5000)
  public void testShouldTake2SecondsToEmitIfLowerThanMinStartup() throws Exception {
    UnicastProcessor<DiscoveryEvent> events = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    long start = System.currentTimeMillis();
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(() -> events, address -> null, 2, 3);

    factory.get().doOnSuccess(s -> latch.countDown()).subscribe();
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)));
    latch.await();

    Assert.assertTrue((System.currentTimeMillis() - start) > 1900);
  }

  @Test(timeout = 5000)
  public void testShouldTakeLessThan2SecondsToEmitIfLowerThanMinStartup() throws Exception {
    UnicastProcessor<DiscoveryEvent> events = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    long start = System.currentTimeMillis();
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(() -> events, address -> null, 1, 4);

    factory.get().doOnSuccess(s -> latch.countDown()).subscribe();
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)));
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)));
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8083)));
    latch.await();

    Assert.assertTrue((System.currentTimeMillis() - start) < 3_000);
  }

  @Test(timeout = 5000)
  public void testShouldEmitThreeUniqueFactories() throws Exception {
    Flux<DiscoveryEvent> events =
        Flux.just(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));

    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(() -> events, address -> null, 1, 1);

    long count = Flux.range(1, 20).flatMap(i -> factory.get()).distinct().count().block();

    System.out.println(count);
  }

  @Test(timeout = 5000)
  public void testShouldSelectOneMoreThanAnother() throws Exception {
    Flux<DiscoveryEvent> events =
        Flux.just(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));

    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(() -> events, address -> null, 1, 1);

    WeightedClientTransportSupplier block = factory.get().block();
    for (int i = 0; i < 2000; i++) {
      block.apply(Flux.never());
    }

    AtomicInteger i1 = new AtomicInteger();
    AtomicInteger i2 = new AtomicInteger();

    Flux.range(1, 20_000)
        .flatMap(i -> factory.get())
        .doOnNext(
            s -> {
              s.apply(Flux.never());
              if (s == block) {
                i1.incrementAndGet();
              } else {
                i2.incrementAndGet();
              }
            })
        .blockLast();

    Assert.assertTrue(i1.get() > 0);
    Assert.assertTrue(i1.get() < i2.get());
    Assert.assertTrue(i1.get() + i2.get() == 20_000);
  }

  @Test(timeout = 5000)
  @Ignore
  public void testShouldEventuallySelectOnlySocket() throws Exception {
    Flux<DiscoveryEvent> events =
        Flux.just(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8001)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));

    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(
            () -> events,
            (SocketAddress address) -> () -> TcpClientTransport.create((InetSocketAddress) address),
            1,
            3);

    WeightedClientTransportSupplier block = factory.get().block();
    for (int i = 0; i < 2000; i++) {
      block.apply(Flux.never());
    }

    AtomicInteger i1 = new AtomicInteger();
    AtomicInteger i2 = new AtomicInteger();

    Flux.range(1, 20_000)
        .flatMap(i -> factory.get())
        .doOnNext(
            s -> {
              s.apply(Flux.never());
              if (s == block) {
                i1.incrementAndGet();
              } else {
                i2.incrementAndGet();
              }
            })
        .blockLast();

    Assert.assertTrue(i1.get() > 0);
    Assert.assertTrue(i1.get() < i2.get());
    Assert.assertTrue(i1.get() + i2.get() == 20_000);
  }

  @Ignore
  @Test(timeout = 5000)
  public void testShouldEmitThreeRSocketsBeforeTimeout() {
    InetSocketAddress localhost = InetSocketAddress.createUnresolved("localhost", 8001);
    List<SocketAddress> list = Arrays.asList(localhost);
    TimebasedIdGenerator idGenerator = new TimebasedIdGenerator(1);

    int length =
        DestinationSetupFlyweight.computeLength(
            false, "testShouldEmitThreeRSockets-dest", "testShouldEmitThreeRSockets-group");

    byte[] decode = Base64.getDecoder().decode(accessToken);

    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
    DestinationSetupFlyweight.encode(
        byteBuf,
        Unpooled.EMPTY_BUFFER,
        Unpooled.wrappedBuffer(decode),
        idGenerator.nextId(),
        accessKey,
        "testShouldEmitThreeRSockets-dest",
        "testShouldEmitThreeRSockets-group");

    RSocket rSocket =
        RSocketFactory.connect()
            .setupPayload(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf))
            .transport(TcpClientTransport.create(localhost))
            .start()
            .block();

    Supplier<SecureRSocket> supplier =
        () -> {
          return new Proxy(rSocket);
        };

    RouterInfoSocketAddressFactory factory =
        new RouterInfoSocketAddressFactory(list, null, idGenerator);

    ClientTransportSupplierFactory cFactory =
        new ClientTransportSupplierFactory(
            factory, socketAddress -> createClientTransport(socketAddress), 1, 3);

    long count =
        Flux.range(1, 128)
            .flatMap(i -> cFactory.get().subscribeOn(Schedulers.parallel()))
            .distinct(ws -> ((InetSocketAddress) ws.getSocketAddress()).getPort())
            .count()
            .block();

    Assert.assertEquals(3, count);
  }

  private Supplier<ClientTransport> createClientTransport(SocketAddress address) {
    return () -> {
      InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
      NioEventLoopGroup group =
          new NioEventLoopGroup(
              Runtime.getRuntime().availableProcessors(), ForkJoinPool.commonPool());
      TcpClient client =
          TcpClient.builder()
              .options(
                  options -> {
                    options.disablePool();
                    options.eventLoopGroup(group);
                    options.connectAddress(() -> address);
                  })
              .build();

      return TcpClientTransport.create(client);
    };
  }

  class Proxy extends RSocketProxy implements SecureRSocket {
    public Proxy(RSocket source) {
      super(source);
    }

    @Override
    public SessionUtil getSessionUtil() {
      return null;
    }

    @Override
    public Mono<AtomicLong> getCurrentSessionCounter() {
      return null;
    }

    @Override
    public Mono<byte[]> getCurrentSessionToken() {
      return null;
    }
  }
}
