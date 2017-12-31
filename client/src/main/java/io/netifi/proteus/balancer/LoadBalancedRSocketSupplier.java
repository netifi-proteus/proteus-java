package io.netifi.proteus.balancer;

import io.netifi.proteus.rs.SecureRSocket;
import io.netifi.proteus.rs.WeightedRSocket;
import io.netifi.proteus.rs.WeightedReconnectingRSocket;
import io.netifi.proteus.stats.FrugalQuantile;
import io.netifi.proteus.stats.Quantile;
import io.netifi.proteus.util.Xoroshiro128PlusRandom;
import io.rsocket.Closeable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Load Balancer that selects the next RSocket to use. Uses Power of Two to select two {@link
 * WeightedRSocket} and then compares there weights. The RSocket with the higher weight is selected.
 */
public class LoadBalancedRSocketSupplier implements Supplier<SecureRSocket>, Closeable {

  private static final double DEFAULT_EXP_FACTOR = 4.0;
  private static final double DEFAULT_LOWER_QUANTILE = 0.2;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int EFFORT = 5;
  final Quantile lowerQuantile;
  final Quantile higherQuantile;
  private final double expFactor;
  private final WeightedReconnectingRSocket[] members;
  private final Xoroshiro128PlusRandom rnd = new Xoroshiro128PlusRandom(System.nanoTime());
  private MonoProcessor<Void> onClose;

  public LoadBalancedRSocketSupplier(
      int poolSize, BiFunction<Quantile, Quantile, WeightedReconnectingRSocket> socketSupplier) {
    this(
        poolSize,
        socketSupplier,
        DEFAULT_EXP_FACTOR,
        new FrugalQuantile(DEFAULT_LOWER_QUANTILE),
        new FrugalQuantile(DEFAULT_HIGHER_QUANTILE));
  }

  public LoadBalancedRSocketSupplier(
      int poolSize,
      BiFunction<Quantile, Quantile, WeightedReconnectingRSocket> socketSupplier,
      double expFactor,
      Quantile lowerQuantile,
      Quantile higherQuantile) {
    if (poolSize < 0) {
      throw new IllegalStateException("poolSize must be 1 or greater");
    }
    this.expFactor = expFactor;
    this.lowerQuantile = lowerQuantile;
    this.higherQuantile = higherQuantile;
    this.onClose = MonoProcessor.create();
    this.members = new WeightedReconnectingRSocket[poolSize];

    for (int i = 0; i < poolSize; i++) {
      WeightedReconnectingRSocket rSocket = socketSupplier.apply(lowerQuantile, higherQuantile);
      members[i] = rSocket;
    }
  }

  @Override
  public SecureRSocket get() {
    SecureRSocket rSocket;
    WeightedReconnectingRSocket[] _m = members;
    int size = _m.length;
    if (size == 1) {
      rSocket = _m[0];
    } else {
      WeightedReconnectingRSocket rsc1 = null;
      WeightedReconnectingRSocket rsc2 = null;

      for (int i = 0; i < EFFORT; i++) {
        int i1;
        int i2;
        synchronized (this) {
          i1 = rnd.nextInt(size);
          i2 = rnd.nextInt(size - 1);
        }
        if (i2 >= i1) {
          i2++;
        }
        rsc1 = _m[i1];
        rsc2 = _m[i2];
        if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
          break;
        }
      }

      double w1 = algorithmicWeight(rsc1);
      double w2 = algorithmicWeight(rsc2);
      if (w1 < w2) {
        rSocket = rsc2;
      } else {
        rSocket = rsc1;
      }
    }

    return rSocket;
  }

  double algorithmicWeight(WeightedRSocket socket) {
    if (socket == null || socket.availability() == 0.0) {
      return 0.0;
    }
    int pendings = socket.pending();
    double latency = socket.predictedLatency();

    double low = lowerQuantile.estimation();
    double high =
        Math.max(
            higherQuantile.estimation(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      latency /= calculateFactor(low, latency, bandWidth);
    } else if (latency > high) {
      latency *= calculateFactor(latency, high, bandWidth);
    }

    return socket.availability() * 1.0 / (1.0 + latency * (pendings + 1));
  }

  private double calculateFactor(double u, double l, double bandWidth) {
    double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, expFactor);
  }

  @Override
  public void dispose() {
    for (WeightedReconnectingRSocket rSocket : members) {
      rSocket.dispose();
    }
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
