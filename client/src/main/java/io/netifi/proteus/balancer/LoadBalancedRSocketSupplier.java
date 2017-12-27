package io.netifi.proteus.balancer;

import io.netifi.proteus.stats.FrugalQuantile;
import io.netifi.proteus.stats.Quantile;
import io.netifi.proteus.rs.SecureRSocket;
import io.netifi.proteus.rs.WeightedRSocket;
import io.netifi.proteus.rs.WeightedReconnectingRSocket;
import io.rsocket.Closeable;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Load Balancer that selects the next RSocket to use. Uses Power of Two to select two {@link
 * WeightedRSocket} and then compares there weights. The RSocket with the higher weight is selected.
 */
public class LoadBalancedRSocketSupplier implements Supplier<SecureRSocket>, Closeable {

  private static final double DEFAULT_EXP_FACTOR = 4.0;
  private static final double DEFAULT_LOWER_QUANTILE = 0.2;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int EFFORT = 5;
  private final double expFactor;
  private final Quantile lowerQuantile;
  private final Quantile higherQuantile;
  private final WeightedReconnectingRSocket[] members;
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

      Random rng = ThreadLocalRandom.current();
      for (int i = 0; i < EFFORT; i++) {
        int i1 = rng.nextInt(size);
        int i2 = rng.nextInt(size - 1);
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

  private double algorithmicWeight(WeightedRSocket socket) {
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
      double alpha = (low - latency) / bandWidth;
      double bonusFactor = Math.pow(1 + alpha, expFactor);
      latency /= bonusFactor;
    } else if (latency > high) {
      double alpha = (latency - high) / bandWidth;
      double penaltyFactor = Math.pow(1 + alpha, expFactor);
      latency *= penaltyFactor;
    }

    return socket.availability() * 1.0 / (1.0 + latency * (pendings + 1));
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
