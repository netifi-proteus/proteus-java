package io.netifi.proteus.rsocket.transport;

import io.rsocket.Closeable;
import io.rsocket.rpc.stats.Ewma;
import io.rsocket.transport.ClientTransport;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class WeightedClientTransportSupplier implements Supplier<ClientTransport>, Closeable {

  private static final Logger logger =
      LoggerFactory.getLogger(WeightedClientTransportSupplier.class);
  private static AtomicInteger SUPPLIER_ID = new AtomicInteger();
  private final MonoProcessor<Void> onClose;
  private final int supplierId;
  private final Function<SocketAddress, ClientTransport> clientTransportFunction;
  private final Ewma errorPercentage;
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
    this.activeConnections = new AtomicInteger();
    this.onClose = MonoProcessor.create();
  }

  /** Marks the connection as active and in-use. */
  public void activate() {
    activeConnections.incrementAndGet();
  }

  @Override
  public ClientTransport get() {
    if (onClose.isDisposed()) {
      throw new IllegalStateException("WeightedClientTransportSupplier is closed");
    }

    int i = activeConnections.get();

    return () ->
        clientTransportFunction
            .apply(socketAddress)
            .connect()
            .doOnNext(
                duplexConnection -> {
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
                          })
                      .subscribe();

                  errorPercentage.insert(1.0);
                })
            .doOnError(t -> errorPercentage.insert(0.0));
  }

  private double errorPercentage() {
    return errorPercentage.value();
  }

  int activeConnections() {
    return activeConnections.get();
  }

  public double weight() {
    double e = errorPercentage();
    int a = activeConnections();

    if (e == 1.0) {
      return a;
    } else {
      return Math.exp(1 / (1 - e)) * a;
    }
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
        + ", socketAddress="
        + socketAddress
        + ", activeConnections="
        + activeConnections
        + '}';
  }
}
