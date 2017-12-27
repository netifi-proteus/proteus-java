package io.netifi.proteus.discovery;

import static io.netifi.proteus.util.DnsUntil.toIpAddress;
import static io.netifi.proteus.util.StringUtil.hasLetters;

import io.netifi.proteus.frames.RouterNodeInfoEventType;
import io.netifi.proteus.frames.RouterNodeInfoFlyweight;
import io.netifi.proteus.frames.RouterNodeInfoResultFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

/**
 * SocketAddressFactory implementation that streams RouterInformation events and uses that to create
 * join and leave events.
 */
public class RouterInfoSocketAddressFactory implements SocketAddressFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(RouterInfoSocketAddressFactory.class);
  final List<DiscoveryEvent> addEvents;
  private final List<SocketAddress> seedAddresses;
  private Function<SocketAddress, Mono<RSocket>> rSocketFactory;
  private TimebasedIdGenerator idGenerator;

  public RouterInfoSocketAddressFactory(
      List<SocketAddress> seedAddresses,
      Function<SocketAddress, Mono<RSocket>> rSocketFactory,
      TimebasedIdGenerator idGenerator) {
    this.seedAddresses = Collections.unmodifiableList(seedAddresses);
    this.addEvents = new ArrayList<>();
    this.rSocketFactory = rSocketFactory;
    this.idGenerator = idGenerator;
  }

  SocketAddress select() {
    SocketAddress socketAddress;
    if (seedAddresses.size() == 1) {
      socketAddress = seedAddresses.get(0);
    } else {
      int i = ThreadLocalRandom.current().nextInt(seedAddresses.size());
      socketAddress = seedAddresses.get(i);
    }

    logger.debug("streaming router info events from {}", socketAddress);

    return socketAddress;
  }

  List<DiscoveryEvent> getRemoveEvents(
      List<DiscoveryEvent> seedEvents, List<DiscoveryEvent> oldEvents) {
    HashSet<DiscoveryEvent> set = new HashSet<>();
    set.addAll(oldEvents);
    set.removeAll(seedEvents);

    return set.stream()
        .map(d -> DiscoveryEvent.remove(d.getId(), d.getAddress()))
        .collect(Collectors.toList());
  }

  Mono<RSocket> getRSocket() {
    SocketAddress address = select();
    return rSocketFactory.apply(address);
  }

  @Override
  public Flux<DiscoveryEvent> get() {
    return getRSocket()
        .flatMapMany(
            rSocket -> {
              UnicastProcessor<DiscoveryEvent> seedRemoveEvents = UnicastProcessor.create();
              List<DiscoveryEvent> seedEvents = Collections.synchronizedList(new ArrayList<>());
              Flux<DiscoveryEvent> discoveryEventFlux =
                  getRouterInfo(rSocket)
                      .map(
                          payload -> {
                            ByteBuf metadata = payload.sliceMetadata();
                            RouterNodeInfoEventType type =
                                RouterNodeInfoResultFlyweight.eventType(metadata);
                            String routerId = RouterNodeInfoResultFlyweight.routerId(metadata);
                            String host = RouterNodeInfoResultFlyweight.routerAddress(metadata);
                            int port = RouterNodeInfoResultFlyweight.routerPort(metadata);

                            InetSocketAddress address;
                            if (hasLetters(host)) {
                              address = toIpAddress(host, port);
                              logger.debug("found host name, resolving to ip address - {}", host);
                            } else {
                              address = InetSocketAddress.createUnresolved(host, port);
                            }

                            logger.debug(
                                "received router event {} for host {} and port {}",
                                type,
                                host,
                                port);

                            switch (type) {
                              case SEED_COMPLETE:
                              case SEED_NEXT:
                              case JOIN:
                                DiscoveryEvent add = DiscoveryEvent.add(routerId, address);
                                if (type == RouterNodeInfoEventType.SEED_NEXT) {
                                  seedEvents.add(add);
                                } else if (type == RouterNodeInfoEventType.SEED_COMPLETE) {
                                  seedEvents.add(add);

                                  getRemoveEvents(seedEvents, addEvents)
                                      .forEach(seedRemoveEvents::onNext);
                                  seedRemoveEvents.onComplete();
                                }

                                return add;
                              case LEAVE:
                                return DiscoveryEvent.remove(routerId, address);
                              default:
                                throw new IllegalStateException("unknown discovery event");
                            }
                          });

              return Flux.merge(discoveryEventFlux, seedRemoveEvents)
                  .doOnNext(this::processDiscoveryEvent);
            });
  }

  Flux<Payload> getRouterInfo(RSocket rSocket) {
    int length = RouterNodeInfoFlyweight.computeLength();
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
    RouterNodeInfoFlyweight.encode(byteBuf, idGenerator.nextId());
    return rSocket.requestStream(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf));
  }

  synchronized void processDiscoveryEvent(DiscoveryEvent d) {
    switch (d.getType()) {
      case Add:
        Optional<DiscoveryEvent> first =
            addEvents
                .stream()
                .filter(
                    de -> d.getId().equals(de.getId()) || d.getAddress().equals(de.getAddress()))
                .findFirst();

        if (!first.isPresent()) {
          addEvents.add(d);
        }
        break;

      case Remove:
        addEvents.removeIf(Predicate.isEqual(d));
        break;
      default:
        throw new IllegalStateException("illegal state exception");
    }
  }
}
