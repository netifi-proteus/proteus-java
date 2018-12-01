package io.netifi.proteus.rsocket.transport;

import io.netifi.proteus.rsocket.WeightedRSocket;
import io.rsocket.Closeable;
import io.rsocket.rpc.stats.Ewma;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.Clock;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class WeightedClientTransportSupplier
    implements Function<Flux<WeightedRSocket>, Supplier<ClientTransport>>, Closeable {

  private static final Logger logger =
      LoggerFactory.getLogger(WeightedClientTransportSupplier.class);
  private static AtomicInteger SUPPLIER_ID = new AtomicInteger();
  private final MonoProcessor<Void> onClose;
  private final int supplierId;
  private final Function<SocketAddress, ClientTransport> clientTransportFunction;
  private final Ewma errorPercentage;
  private final Ewma latency;
  private final SocketAddress socketAddress;
  private final AtomicInteger activeConnections;

  @SuppressWarnings("unused")
  private volatile int selectedCounter = 1;

  public WeightedClientTransportSupplier(
      SocketAddress socketAddress,
      Function<SocketAddress, ClientTransport> clientTransportFunction) {
    this.supplierId = SUPPLIER_ID.incrementAndGet();
    this.clientTransportFunction = clientTransportFunction;
    this.socketAddress = socketAddress;
    this.errorPercentage = new Ewma(5, TimeUnit.SECONDS, 1.0);
    this.latency = new Ewma(1, TimeUnit.MINUTES, Clock.unit().convert(1L, TimeUnit.SECONDS));
    this.activeConnections = new AtomicInteger();
    this.onClose = MonoProcessor.create();
  }

  @Override
  public Supplier<ClientTransport> apply(Flux<WeightedRSocket> weightedSocketFlux) {
    if (onClose.isDisposed()) {
      throw new IllegalStateException("WeightedClientTransportSupplier is closed");
    }

    Disposable subscribe =
        weightedSocketFlux
            .doOnNext(
                weightedRSocket -> {
                  if (logger.isTraceEnabled()) {
                    logger.trace("recording stats {}", weightedRSocket.toString());
                  }
                  double e = weightedRSocket.errorPercentage();
                  double i = weightedRSocket.higherQuantileLatency();

                  errorPercentage.insert(e);
                  latency.insert(i);
                })
            .subscribe();

    return () ->
        () ->
            clientTransportFunction
                .apply(socketAddress)
                .connect()
                .doOnNext(
                    duplexConnection -> {
                      int i = activeConnections.incrementAndGet();
                      logger.debug(
                          "supplier id - {} - opened connection to {} - active connections {}",
                          supplierId,
                          socketAddress,
                          i);

                      Disposable onCloseDisposable =
                          onClose.doFinally(s -> duplexConnection.dispose()).subscribe();

                      duplexConnection
                          .onClose()
                          .doFinally(
                              s -> {
                                int d = activeConnections.decrementAndGet();
                                logger.debug(
                                    "supplier id - {} - closed connection {} - active connections {}",
                                    supplierId,
                                    socketAddress,
                                    d);
                                onCloseDisposable.dispose();
                                subscribe.dispose();
                              })
                          .subscribe();
                    })
                .doOnError(t -> errorPercentage.insert(1.0));
  }

  public double errorPercentage() {
    return errorPercentage.value();
  }

  public double latency() {
    return latency.value();
  }

  public int activeConnections() {
    return activeConnections.get();
  }

  /**
   * Caculates the weight for a transport supplier based on the error percentage, latency, and
   * number of active connections. The higher weight, the worse the quality the connection is for
   * selection. Lower is better. Error percentages will exponentially effect the weight of the
   * connection.
   *
   * @return a weight based on e^(1 / (1.0 - errorPercentage)) * (1.0 + latency) + (1 + active
   *     connections)
   */
  public double weight() {
    double e = Math.max(0.90, errorPercentage());
    double l = latency();
    int a = activeConnections();

    return Math.exp(1.0 / (1.0 - e)) * (1.0 + l) * (1 + a);
  }

  public SocketAddress getSocketAddress() {
    return socketAddress;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WeightedClientTransportSupplier supplier = (WeightedClientTransportSupplier) o;

    return socketAddress.equals(supplier.socketAddress);
  }

  @Override
  public int hashCode() {
    return socketAddress.hashCode();
  }

  @Override
  public String toString() {
    return "WeightedClientTransportSupplier{"
        + '\''
        + ", supplierId="
        + supplierId
        + ", errorPercentage="
        + errorPercentage
        + ", latency="
        + latency
        + ", socketAddress="
        + socketAddress
        + ", activeConnections="
        + activeConnections
        + ", selectedCounter="
        + selectedCounter
        + '}';
  }
}
