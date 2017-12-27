package io.netifi.proteus.rs;

import io.netifi.proteus.auth.SessionUtil;
import io.netifi.proteus.balancer.LoadBalancedRSocketSupplier;
import io.netifi.proteus.frames.RouteDestinationFlyweight;
import io.netifi.proteus.frames.RouteType;
import io.netifi.proteus.frames.RoutingFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultNetifiSocket implements NetifiSocket {
  private final SessionUtil sessionUtil = SessionUtil.instance();
  private final MonoProcessor<Void> onClose;
  private final ByteBuf route;
  private long accessKey;
  private final String fromDestination;
  private final TimebasedIdGenerator generator;
  private final LoadBalancedRSocketSupplier rSocketSupplier;

  public DefaultNetifiSocket(
      LoadBalancedRSocketSupplier rSocketSupplier,
      long accessKey,
      long fromAccountId,
      String fromDestination,
      String destination,
      String group,
      byte[] accessTokenBytes,
      boolean keepalive,
      TimebasedIdGenerator generator) {
    this.rSocketSupplier = rSocketSupplier;
    this.accessKey = accessKey;
    this.fromDestination = fromDestination;
    this.generator = generator;
    this.onClose = MonoProcessor.create();

    if (destination != null && !destination.equals("")) {
      int length =
          RouteDestinationFlyweight.computeLength(
              RouteType.STREAM_ID_ROUTE, fromDestination, group);
      route = ByteBufAllocator.DEFAULT.directBuffer(length);
      RouteDestinationFlyweight.encodeRouteByDestination(
          route, RouteType.STREAM_ID_ROUTE, fromAccountId, destination, group);
    } else {
      int length = RouteDestinationFlyweight.computeLength(RouteType.STREAM_GROUP_ROUTE, group);
      route = ByteBufAllocator.DEFAULT.directBuffer(length);
      RouteDestinationFlyweight.encodeRouteByGroup(
          route, RouteType.STREAM_GROUP_ROUTE, fromAccountId, group);
    }

    rSocketSupplier.onClose().doFinally(s -> onClose.onComplete()).subscribe();
  }

  @Override
  public double availability() {
    return 1.0;
  }

  public ByteBuf getRoute() {
    return route.asReadOnly();
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      ByteBuf metadataToWrap = payload.sliceMetadata();
      ByteBuf data = payload.sliceData();
      ByteBuf route = getRoute();

      int length = RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

      SecureRSocket secureRSocket = rSocketSupplier.get();
      return secureRSocket
          .getCurrentSessionCounter()
          .flatMap(
              counter -> {
                long count = counter.incrementAndGet();

                return secureRSocket
                    .getCurrentSessionToken()
                    .flatMap(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, data, count);
                          int requestToken =
                              sessionUtil.generateRequestToken(currentRequestToken, data, count);
                          ByteBuf metadata = ByteBufAllocator.DEFAULT.directBuffer(length);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              metadataToWrap);

                          return secureRSocket.fireAndForget(
                              ByteBufPayload.create(payload.sliceData(), metadata));
                        });
              });

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      ByteBuf metadataToWrap = payload.sliceMetadata();
      ByteBuf route = getRoute();
      ByteBuf data = payload.sliceData();
      int length = RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

      SecureRSocket secureRSocket = rSocketSupplier.get();
      return secureRSocket
          .getCurrentSessionCounter()
          .flatMap(
              counter -> {
                long count = counter.incrementAndGet();

                return secureRSocket
                    .getCurrentSessionToken()
                    .flatMap(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, data, count);
                          int requestToken =
                              sessionUtil.generateRequestToken(currentRequestToken, data, count);
                          ByteBuf metadata = ByteBufAllocator.DEFAULT.directBuffer(length);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              metadataToWrap);

                          return secureRSocket.requestResponse(
                              ByteBufPayload.create(payload.sliceData(), metadata));
                        });
              });
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      ByteBuf metadataToWrap = payload.sliceMetadata();
      ByteBuf route = getRoute();
      ByteBuf data = payload.sliceData();

      int length = RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

      SecureRSocket secureRSocket = rSocketSupplier.get();
      return secureRSocket
          .getCurrentSessionCounter()
          .flatMapMany(
              counter -> {
                long count = counter.incrementAndGet();

                return secureRSocket
                    .getCurrentSessionToken()
                    .flatMapMany(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, data, count);
                          int requestToken =
                              sessionUtil.generateRequestToken(currentRequestToken, data, count);
                          ByteBuf metadata = ByteBufAllocator.DEFAULT.directBuffer(length);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              metadataToWrap);

                          return secureRSocket.requestStream(
                              ByteBufPayload.create(payload.sliceData(), metadata));
                        });
              });

    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    SecureRSocket secureRSocket = rSocketSupplier.get();
    ByteBuf route = getRoute();
    Flux<Payload> payloadFlux =
        Flux.from(payloads)
            .flatMap(
                payload -> {
                  ByteBuf data = payload.sliceData();
                  ByteBuf metadataToWrap = payload.sliceMetadata();
                  int length =
                      RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

                  return secureRSocket
                      .getCurrentSessionCounter()
                      .flatMapMany(
                          counter -> {
                            long count = counter.incrementAndGet();

                            return secureRSocket
                                .getCurrentSessionToken()
                                .map(
                                    key -> {
                                      byte[] currentRequestToken =
                                          sessionUtil.generateSessionToken(key, data, count);
                                      int requestToken =
                                          sessionUtil.generateRequestToken(
                                              currentRequestToken, data, count);
                                      ByteBuf metadata =
                                          ByteBufAllocator.DEFAULT.directBuffer(length);
                                      RoutingFlyweight.encode(
                                          metadata,
                                          true,
                                          requestToken,
                                          accessKey,
                                          fromDestination,
                                          generator.nextId(),
                                          route,
                                          metadataToWrap);

                                      return ByteBufPayload.create(payload.sliceData(), metadata);
                                    });
                          });
                });

    return secureRSocket.requestChannel(payloadFlux);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      ByteBuf route = getRoute();
      ByteBuf unwrappedMetadata = payload.sliceMetadata();
      ByteBuf data = payload.sliceData();

      int length = RoutingFlyweight.computeLength(true, fromDestination, route);
      SecureRSocket secureRSocket = rSocketSupplier.get();

      return secureRSocket
          .getCurrentSessionCounter()
          .flatMap(
              counter -> {
                long count = counter.incrementAndGet();

                return secureRSocket
                    .getCurrentSessionToken()
                    .flatMap(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, data, count);
                          int requestToken =
                              sessionUtil.generateRequestToken(currentRequestToken, data, count);
                          ByteBuf metadata = ByteBufAllocator.DEFAULT.directBuffer(length);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              unwrappedMetadata);

                          return secureRSocket.metadataPush(
                              ByteBufPayload.create(payload.sliceData(), metadata));
                        });
              });

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
}
