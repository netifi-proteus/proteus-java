package io.netifi.proteus.admin.connection;

import io.netifi.proteus.admin.rs.AdminRSocket;
import io.netifi.proteus.discovery.DiscoveryEvent;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.rsocket.Closeable;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultConnectionManager implements ConnectionManager, Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionManager.class);
  private static final int EFFORT = 5;
  private final List<AdminRSocket> rSockets;
  private final Function<SocketAddress, Mono<AdminRSocket>> adminSocketFactory;
  private final MonoProcessor<Void> onClose;
  private final DirectProcessor<AdminRSocket> newSocketProcessor;
  private final RouterInfoSocketAddressFactory socketAddressFactory;
  private MonoProcessor<Void> onConnectionPresent;
  private boolean missed;

  public DefaultConnectionManager(
      TimebasedIdGenerator idGenerator,
      Function<SocketAddress, Mono<AdminRSocket>> adminSocketFactory,
      List<SocketAddress> socketAddresses) {
    this.newSocketProcessor = DirectProcessor.create();
    this.rSockets = new ArrayList<>();
    this.adminSocketFactory = adminSocketFactory;
    this.onClose = MonoProcessor.create();

    reset();

    this.socketAddressFactory = new RouterInfoSocketAddressFactory(this, idGenerator);

    Flux<AdminRSocket> seedFlux =
        Flux.fromIterable(socketAddresses).map(DiscoveryEvent::add).flatMap(this::handelEvent);

    Flux<AdminRSocket> adminRSocketFlux = socketAddressFactory.get().flatMap(this::handelEvent);

    Disposable subscribe =
        Flux.merge(seedFlux, adminRSocketFlux)
            .doOnNext(this::addNewAdminSocket)
            .onErrorResume(
                t -> {
                  logger.error("error updating Admin RSockets", t.getMessage());
                  return Mono.delay(Duration.ofSeconds(5)).then(Mono.error(t));
                })
            .retry()
            .repeat()
            .subscribe();

    onClose.doFinally(s -> subscribe.dispose()).subscribe();
  }

  private void addNewAdminSocket(AdminRSocket adminRSocket) {
    boolean add;
    MonoProcessor<Void> p;
    synchronized (this) {
      p = onConnectionPresent;
      Optional<AdminRSocket> first =
          rSockets
              .stream()
              .filter(s -> s.getRouterId().equals(adminRSocket.getRouterId()))
              .findFirst();
      if (!first.isPresent()) {
        add = true;
        rSockets.add(adminRSocket);
      } else {
        add = false;
        logger.debug(
            "connection already present for router id {} to address {}",
            adminRSocket.getRouterId(),
            adminRSocket.getSocketAddress());
        adminRSocket.dispose();
      }
    }

    if (add) {
      newSocketProcessor.onNext(adminRSocket);

      if (!p.isDisposed()) {
        logger.debug("notifying an AdminRSocket is present");
        p.onComplete();
      }
    }
  }

  private synchronized Mono<AdminRSocket> handelEvent(DiscoveryEvent event) {
    missed = true;
    Mono<AdminRSocket> mono;
    String routerId = event.getId();
    SocketAddress address = event.getAddress();
    if (event.getType() == DiscoveryEvent.DiscoveryEventType.Add) {
      Optional<AdminRSocket> first =
          rSockets.stream().filter(s -> s.getRouterId().equals(routerId)).findFirst();

      if (first.isPresent()) {
        return Mono.empty();
      } else {
        logger.debug("adding AdminRSocket for socket address {}", address);
        mono =
            adminSocketFactory
                .apply(address)
                .flatMap(adminRSocket -> adminRSocket.onReady().then(Mono.just(adminRSocket)));
      }
    } else {
      logger.debug("removing AdminRSocket for socket address {}", address);
      Optional<AdminRSocket> first =
          rSockets.stream().filter(s -> s.getRouterId().equals(routerId)).findFirst();

      if (first.isPresent()) {
        AdminRSocket adminRSocket = first.get();
        rSockets.remove(adminRSocket);
        adminRSocket.dispose();
      }

      mono =
          Mono.fromRunnable(
              () -> {
                if (rSockets.isEmpty()) {
                  logger.debug(
                      "no client transport suppliers present, reset to wait an AdminRSocket");
                  reset();
                }
              });
    }

    return mono;
  }

  private synchronized void reset() {
    if (onConnectionPresent == null || onConnectionPresent.isTerminated()) {
      this.onConnectionPresent = MonoProcessor.create();
    }
  }

  private synchronized Mono<Void> getOnConnectionPresent() {
    return onConnectionPresent;
  }

  private AdminRSocket get() {
    AdminRSocket rSocket = null;
    for (; ; ) {
      List<AdminRSocket> _rSockets;
      synchronized (this) {
        missed = false;
        _rSockets = rSockets;
      }
      for (int i = 0; i < EFFORT; i++) {
        int size = _rSockets.size();
        if (size == 1) {
          rSocket = _rSockets.get(0);
        } else {
          int k = ThreadLocalRandom.current().nextInt(0, size);
          rSocket = _rSockets.get(k);
        }

        if (rSocket.availability() > 0.0) {
          break;
        }
      }
      synchronized (this) {
        if (!missed) {
          break;
        }
      }
    }

    return rSocket;
  }

  @Override
  public Mono<AdminRSocket> getRSocket() {
    return getOnConnectionPresent().then(Mono.fromSupplier(this::get));
  }

  @Override
  public Flux<AdminRSocket> getRSockets() {
    return Flux.merge(Flux.fromIterable(rSockets), newSocketProcessor);
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
