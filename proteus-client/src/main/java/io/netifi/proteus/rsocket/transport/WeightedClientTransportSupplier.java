/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.netifi.proteus.rsocket.transport;

import io.netifi.proteus.broker.info.Broker;
import io.netifi.proteus.common.stats.Ewma;
import io.rsocket.Closeable;
import io.rsocket.transport.ClientTransport;
import java.net.InetSocketAddress;
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
  private final MonoProcessor<Void> onClose;
  private final Function<SocketAddress, ClientTransport> clientTransportFunction;
  private final Ewma errorPercentage;
  private final SocketAddress socketAddress;
  private final AtomicInteger selectCount;
  private final Broker broker;

  public WeightedClientTransportSupplier(
      Broker broker,
      Function<Broker, InetSocketAddress> addressSelector,
      Function<SocketAddress, ClientTransport> clientTransportFunction) {
    this.broker = broker;
    this.clientTransportFunction = clientTransportFunction;
    this.socketAddress = addressSelector.apply(broker);
    this.errorPercentage = new Ewma(5, TimeUnit.SECONDS, 1.0);
    this.selectCount = new AtomicInteger();
    this.onClose = MonoProcessor.create();
  }

  public void select() {
    selectCount.incrementAndGet();
  }

  @Override
  public ClientTransport get() {
    if (onClose.isDisposed()) {
      throw new IllegalStateException("WeightedClientTransportSupplier is closed");
    }

    int i = selectCount.get();

    return () ->
        clientTransportFunction
            .apply(socketAddress)
            .connect()
            .doOnNext(
                duplexConnection -> {
                  logger.debug("opened connection to {} - active connections {}", socketAddress, i);

                  Disposable onCloseDisposable =
                      onClose.doFinally(s -> duplexConnection.dispose()).subscribe();

                  duplexConnection
                      .onClose()
                      .doFinally(
                          s -> {
                            int d = selectCount.decrementAndGet();
                            logger.debug(
                                "closed connection {} - active connections {}", socketAddress, d);
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
    return selectCount.get();
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

  public Broker getBroker() {
    return broker;
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
        + "errorPercentage="
        + errorPercentage
        + ", socketAddress="
        + socketAddress
        + ", selectCount="
        + selectCount
        + '}';
  }
}
