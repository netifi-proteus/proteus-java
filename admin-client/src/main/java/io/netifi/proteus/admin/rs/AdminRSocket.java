package io.netifi.proteus.admin.rs;

import io.netifi.proteus.frames.admin.AdminRouterNodeInfoFlyweight;
import io.netifi.proteus.frames.admin.AdminRouterNodeInfoResultFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.ReplayProcessor;

public class AdminRSocket implements RSocket {
  private static final Logger logger = LoggerFactory.getLogger(AdminRSocket.class);
  private final TimebasedIdGenerator idGenerator;
  private final SocketAddress address;
  private final Function<SocketAddress, Mono<RSocket>> rSocketFactory;
  private final ReplayProcessor<Mono<RSocket>> source;
  private String routerId;
  private MonoProcessor<RSocket> currentSink;
  private volatile double available;

  public AdminRSocket(
      SocketAddress address,
      Function<SocketAddress, Mono<RSocket>> rSocketFactory,
      TimebasedIdGenerator idGenerator) {
    this.address = address;
    this.rSocketFactory = rSocketFactory;
    this.source = ReplayProcessor.cacheLast();
    this.idGenerator = idGenerator;

    resetMono();

    connect(1).subscribe();
  }

  private Mono<RSocket> connect(int retry) {
    if (source.isDisposed()) {
      return Mono.empty();
    }

    return rSocketFactory
        .apply(address)
        .flatMap(
            rSocket -> {
              int length = AdminRouterNodeInfoFlyweight.computeLength();
              ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
              AdminRouterNodeInfoFlyweight.encode(byteBuf, idGenerator.nextId());
              return rSocket
                  .requestResponse(ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf))
                  .doOnNext(
                      payload -> {
                        String routerId =
                            AdminRouterNodeInfoResultFlyweight.routerId(payload.sliceMetadata());
                        setRouterId(routerId);
                      })
                  .map(payload -> rSocket)
                  .timeout(Duration.ofSeconds(10));
            })
        .doOnNext(this::setRSocket)
        .onErrorResume(
            t -> {
              logger.debug(t.getMessage(), t);
              return retryConnection(retry);
            });
  }

  private Mono<RSocket> retryConnection(int retry) {
    logger.debug("delaying retry {} seconds", retry);
    return Mono.delay(Duration.ofSeconds(retry))
        .then(Mono.defer(() -> connect(Math.min(retry + 1, 60))));
  }

  public SocketAddress getSocketAddress() {
    return address;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return getRSocket()
        .flatMap(rSocket -> rSocket.fireAndForget(payload))
        .onErrorResume(
            t -> {
              if (t instanceof ClosedChannelException) {
                logger.debug("caught ClosedChannelException, ignoring");
                return Mono.empty();
              } else {
                return Mono.error(t);
              }
            });
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return getRSocket()
        .flatMap(rSocket -> rSocket.requestResponse(payload))
        .onErrorResume(
            t -> {
              if (t instanceof ClosedChannelException) {
                logger.debug("caught ClosedChannelException, ignoring");
                return Mono.empty();
              } else {
                return Mono.error(t);
              }
            });
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return getRSocket()
        .flatMapMany(rSocket -> rSocket.requestStream(payload))
        .onErrorResume(
            t -> {
              if (t instanceof ClosedChannelException) {
                logger.debug("caught ClosedChannelException, ignoring");
                return Mono.empty();
              } else {
                return Mono.error(t);
              }
            });
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return getRSocket()
        .flatMapMany(rSocket -> rSocket.requestChannel(payloads))
        .onErrorResume(
            t -> {
              if (t instanceof ClosedChannelException) {
                logger.debug("caught ClosedChannelException, ignoring");
                return Mono.empty();
              } else {
                return Mono.error(t);
              }
            });
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return getRSocket()
        .flatMap(rSocket -> rSocket.metadataPush(payload))
        .onErrorResume(
            t -> {
              if (t instanceof ClosedChannelException) {
                logger.debug("caught ClosedChannelException, ignoring");
                return Mono.empty();
              } else {
                return Mono.error(t);
              }
            });
  }

  private void resetMono() {
    MonoProcessor<RSocket> _m;
    synchronized (this) {
      _m = MonoProcessor.create();
      currentSink = _m;
    }

    source.onNext(_m);
  }

  public Mono<Void> onReady() {
    return getRSocket().then();
  }

  private Mono<RSocket> getRSocket() {
    return source.next().flatMap(Function.identity());
  }

  private void setRSocket(RSocket rSocket) {
    MonoProcessor<RSocket> _m;
    synchronized (this) {
      _m = currentSink;
    }

    _m.onNext(rSocket);
    _m.onComplete();

    available = 1.0;

    Disposable subscribe = source.doFinally(s -> rSocket.dispose()).subscribe();

    rSocket
        .onClose()
        .doFinally(
            s -> {
              available = 0.0;
              connect(10).subscribe();
              subscribe.dispose();
              resetMono();
            })
        .subscribe();
  }

  public synchronized String getRouterId() {
    return routerId;
  }

  private synchronized void setRouterId(String routerId) {
    logger.debug("setting router id to {} for address {}", routerId, address);
    this.routerId = routerId;
  }

  @Override
  public double availability() {
    return available;
  }

  @Override
  public void dispose() {
    source.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return source.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return source.then();
  }

  @Override
  public String toString() {
    return "AdminRSocket{" + "address=" + address + ", routerId='" + routerId + '\'' + '}';
  }
}
