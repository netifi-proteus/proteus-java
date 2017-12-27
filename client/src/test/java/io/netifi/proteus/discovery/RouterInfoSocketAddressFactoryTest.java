package io.netifi.proteus.discovery;

import io.netifi.proteus.frames.InfoSetupFlyweight;
import io.netifi.proteus.frames.RouterNodeInfoEventType;
import io.netifi.proteus.frames.RouterNodeInfoResultFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RouterInfoSocketAddressFactoryTest {

  private static final long accessKey = 3855261330795754807L;
  private static final String accessToken = "n9R9042eE1KaLtE56rbWjBIGymo=";

  @Test
  public void testSelectSocketAddress() {
    List<SocketAddress> inetSocketAddresses =
        Arrays.asList(InetSocketAddress.createUnresolved("localhost", 8001));
    RouterInfoSocketAddressFactory info =
        new RouterInfoSocketAddressFactory(inetSocketAddresses, null, null);

    SocketAddress select = info.select();
    Assert.assertNotNull(select);
    Assert.assertEquals(InetSocketAddress.createUnresolved("localhost", 8001), select);
  }

  @Test
  public void testShouldSelectDifferentSocketAddresses() {
    List<SocketAddress> inetSocketAddresses =
        Arrays.asList(
            InetSocketAddress.createUnresolved("localhost", 8001),
            InetSocketAddress.createUnresolved("localhost", 8002),
            InetSocketAddress.createUnresolved("localhost", 8003),
            InetSocketAddress.createUnresolved("localhost", 8004),
            InetSocketAddress.createUnresolved("localhost", 8005),
            InetSocketAddress.createUnresolved("localhost", 8006),
            InetSocketAddress.createUnresolved("localhost", 8007),
            InetSocketAddress.createUnresolved("localhost", 8008));

    RouterInfoSocketAddressFactory info =
        new RouterInfoSocketAddressFactory(inetSocketAddresses, null, null);

    int count = 0;
    for (int i = 0; i < 1000; i++) {
      SocketAddress select = info.select();
      InetSocketAddress address = (InetSocketAddress) select;
      if (address.getPort() == 8001) {
        count++;
      }
    }

    Assert.assertNotEquals(0, count);
    Assert.assertTrue(count < 1_000);
  }

  @Test
  public void testShouldDiffEvents() {
    RouterInfoSocketAddressFactory info = Mockito.mock(RouterInfoSocketAddressFactory.class);
    Mockito.when(info.getRemoveEvents(Mockito.anyList(), Mockito.anyList())).thenCallRealMethod();

    List<DiscoveryEvent> discoveryEvents =
        Arrays.asList(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8001)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8002)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8003)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8004)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8005)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8006)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8007)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8008)));

    List<DiscoveryEvent> snapshot =
        Arrays.asList(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8001)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8002)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8003)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8004)));

    List<DiscoveryEvent> result = info.getRemoveEvents(snapshot, discoveryEvents);
    Assert.assertEquals(4, result.size());

    Optional<DiscoveryEvent> first =
        result
            .stream()
            .filter(
                d -> {
                  InetSocketAddress address = (InetSocketAddress) d.getAddress();
                  return address.getPort() == 8005;
                })
            .findFirst();

    Assert.assertTrue(first.isPresent());
  }

  @Test
  public void testRouterShouldEmitFourSeedAddEvents() {
    List<SocketAddress> seed = Arrays.asList(InetSocketAddress.createUnresolved("localhost", 8001));

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestStream(Mockito.any(Payload.class)))
        .thenReturn(getRouterInfoSeedOnlyPayloads());

    RouterInfoSocketAddressFactory factory =
        new RouterInfoSocketAddressFactory(
            seed, address -> Mono.just(mock), new TimebasedIdGenerator(1));
    List<DiscoveryEvent> result = factory.get().buffer().blockLast();

    Assert.assertEquals(4, result.size());

    Optional<DiscoveryEvent> first =
        result
            .stream()
            .filter(
                d -> {
                  InetSocketAddress address = (InetSocketAddress) d.getAddress();
                  return address.getPort() == 8001;
                })
            .findFirst();

    Assert.assertTrue(first.isPresent());
  }

  @Test
  public void testShouldEmitFourSeedAddEventsAndFourRemoveEvent() {
    List<SocketAddress> seed = Arrays.asList(InetSocketAddress.createUnresolved("localhost", 8001));

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestStream(Mockito.any(Payload.class)))
        .thenReturn(getRouterInfoSeedOnlyPayloads());

    RouterInfoSocketAddressFactory factory =
        new RouterInfoSocketAddressFactory(
            seed, address -> Mono.just(mock), new TimebasedIdGenerator(1));

    List<DiscoveryEvent> discoveryEvents =
        Arrays.asList(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8001)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8002)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8003)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8004)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8005)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8006)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8007)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("127.0.0.1", 8008)));

    factory.addEvents.addAll(discoveryEvents);

    List<DiscoveryEvent> result = factory.get().buffer().blockLast();

    Assert.assertEquals(8, result.size());

    Optional<DiscoveryEvent> first =
        result
            .stream()
            .filter(
                d -> {
                  InetSocketAddress address = (InetSocketAddress) d.getAddress();
                  return address.getPort() == 8001;
                })
            .findFirst();

    Assert.assertTrue(first.isPresent());

    first =
        result
            .stream()
            .filter(
                d -> {
                  InetSocketAddress address = (InetSocketAddress) d.getAddress();
                  return address.getPort() == 8008
                      && d.getType() == DiscoveryEvent.DiscoveryEventType.Remove;
                })
            .findFirst();

    Assert.assertTrue(first.isPresent());
  }

  @Test
  public void testRouterShouldEmitFourSeedAddEventsAndFourAddEvents() {
    List<SocketAddress> seed = Arrays.asList(InetSocketAddress.createUnresolved("localhost", 8001));

    RSocket mock = Mockito.mock(RSocket.class);
    Mockito.when(mock.requestStream(Mockito.any(Payload.class)))
        .thenReturn(getRouterInfoPayloads());

    RouterInfoSocketAddressFactory factory =
        new RouterInfoSocketAddressFactory(
            seed, address -> Mono.just(mock), new TimebasedIdGenerator(1));
    List<DiscoveryEvent> result = factory.get().buffer().blockLast();

    Assert.assertEquals(8, result.size());

    Optional<DiscoveryEvent> first =
        result
            .stream()
            .filter(
                d -> {
                  InetSocketAddress address = (InetSocketAddress) d.getAddress();
                  return address.getPort() == 8001;
                })
            .findFirst();

    Assert.assertTrue(first.isPresent());

    first =
        result
            .stream()
            .filter(
                d -> {
                  InetSocketAddress address = (InetSocketAddress) d.getAddress();
                  return address.getPort() == 8008;
                })
            .findFirst();

    Assert.assertTrue(first.isPresent());
  }

  @Ignore
  @Test
  public void testIntegration() {
    Function<SocketAddress, Mono<RSocket>> f =
        address -> {
          int length =
              InfoSetupFlyweight.computeLength(
                  false, "testIntegration-dest", "testIntegration-group");

          ByteBuf metadata = ByteBufAllocator.DEFAULT.directBuffer(length);
          InfoSetupFlyweight.encode(
              metadata,
              Unpooled.EMPTY_BUFFER,
              Unpooled.wrappedBuffer(Base64.getDecoder().decode(accessToken)),
              1,
              accessKey,
              "testIntegration-dest",
              "testIntegration-group");
          Payload payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata);

          return RSocketFactory.connect()
              .setupPayload(payload)
              .transport(TcpClientTransport.create((InetSocketAddress) address))
              .start();
        };

    List<SocketAddress> seed = Arrays.asList(InetSocketAddress.createUnresolved("localhost", 8001));

    RouterInfoSocketAddressFactory factory =
        new RouterInfoSocketAddressFactory(seed, f, new TimebasedIdGenerator(1));

    factory.get().take(6).blockLast();
  }

  private Flux<Payload> getRouterInfoSeedOnlyPayloads() {
    return Flux.range(1, 4)
        .map(
            i -> {
              String id = "id-" + i;
              String address = "localhost";
              int length =
                  RouterNodeInfoResultFlyweight.computeLength(id, address, address, address);
              ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
              RouterNodeInfoResultFlyweight.encode(
                  byteBuf,
                  i == 4
                      ? RouterNodeInfoEventType.SEED_COMPLETE
                      : RouterNodeInfoEventType.SEED_NEXT,
                  id,
                  address,
                  8000 + i,
                  address,
                  7000 + i,
                  address,
                  6000 + i);

              return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf);
            });
  }

  private Flux<Payload> getRouterInfoPayloads() {
    return Flux.range(1, 8)
        .map(
            i -> {
              String id = "id-" + i;
              String address = "localhost";
              int length =
                  RouterNodeInfoResultFlyweight.computeLength(id, address, address, address);
              ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
              RouterNodeInfoResultFlyweight.encode(
                  byteBuf,
                  i == 4
                      ? RouterNodeInfoEventType.SEED_COMPLETE
                      : (i >= 5 ? RouterNodeInfoEventType.JOIN : RouterNodeInfoEventType.SEED_NEXT),
                  id,
                  address,
                  8000 + i,
                  address,
                  7000 + i,
                  address,
                  6000 + i);

              return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf);
            });
  }
}
