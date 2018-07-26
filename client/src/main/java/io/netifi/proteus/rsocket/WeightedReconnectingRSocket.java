package io.netifi.proteus.rsocket;

import io.netifi.proteus.DestinationNameFactory;
import io.netifi.proteus.exception.TimeoutException;
import io.netifi.proteus.rsocket.transport.WeightedClientTransportSupplier;
import io.netifi.proteus.stats.Ewma;
import io.netifi.proteus.stats.Median;
import io.netifi.proteus.stats.Quantile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.*;
import io.rsocket.util.Clock;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.retry.Jitter;
import reactor.retry.Retry;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A secure RSocket implementation that contains information about its the error percentage and
 * latency. The RSocket will automatically attempt to reconnect if the it loses it's connection. It
 * selects connections from the {@link} to determine with connection to use.
 */
public class WeightedReconnectingRSocket implements WeightedRSocket {
  private static final Logger logger = LoggerFactory.getLogger(WeightedReconnectingRSocket.class);

  private static final RSocket EMPTY_SOCKET = new AbstractRSocket() {};
  private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;
  private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
      Clock.unit().convert(1L, TimeUnit.SECONDS);
  private static final long CONNECTION_ATTEMPT_RESET_TS = Duration.ofMinutes(1).toMillis();
  private final Quantile lowerQuantile;
  private final Quantile higherQuantile;
  private final long inactivityFactor;
  private final long tau;
  private final Ewma errorPercentage;
  private final MonoProcessor<Void> onClose;
  private final Function<String, Payload> setupPayloadSupplier;
  private final BooleanSupplier running;
  private final boolean keepalive;
  private final long tickPeriodSeconds;
  private final long ackTimeoutSeconds;
  private final int missedAcks;
  private final RequestHandlingSocketFactory requestHandlingRSocket;
  private final long accessKey;
  private final ByteBuf accessToken;
  private final Supplier<WeightedClientTransportSupplier> transportSupplier;
  private final DestinationNameFactory destinationNameFactory;
  boolean connecting = false;
  private volatile int pending; // instantaneous rate
  private long errorStamp; // last we got an error
  private long stamp; // last timestamp we sent a request
  private long stamp0; // last timestamp we sent a request or receive a response
  private long duration; // instantaneous cumulative duration
  private Median median;
  private Ewma interArrivalTime;
  private AtomicLong pendingStreams; // number of active streams
  private double availability = 0.0;
  private DirectProcessor<WeightedRSocket> statsProcessor;
  private long lastConnectionAttemptTs = System.currentTimeMillis();
  private long attempts;

  private MonoProcessor<RSocket> currentSink;

  WeightedReconnectingRSocket(
      RequestHandlingSocketFactory requestHandlingRSocket,
      DestinationNameFactory destinationNameFactory,
      Function<String, Payload> setupPayloadSupplier,
      BooleanSupplier running,
      Supplier<WeightedClientTransportSupplier> transportSupplier,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      ByteBuf accessToken,
      Quantile lowerQuantile,
      Quantile higherQuantile,
      int inactivityFactor) {
    this.transportSupplier = transportSupplier;
    this.lowerQuantile = lowerQuantile;
    this.higherQuantile = higherQuantile;
    this.inactivityFactor = inactivityFactor;
    long now = Clock.now();
    this.stamp = now;
    this.errorStamp = now;
    this.stamp0 = now;
    this.duration = 0L;
    this.pending = 0;
    this.median = new Median();
    this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
    this.pendingStreams = new AtomicLong();
    this.errorPercentage = new Ewma(5, TimeUnit.SECONDS, 1.0);
    this.tau = Clock.unit().convert((long) (5 / Math.log(2)), TimeUnit.SECONDS);
    this.requestHandlingRSocket = requestHandlingRSocket;
    this.onClose = MonoProcessor.create();
    this.setupPayloadSupplier = setupPayloadSupplier;
    this.running = running;
    this.keepalive = keepalive;
    this.accessKey = accessKey;
    this.accessToken = accessToken;
    this.tickPeriodSeconds = tickPeriodSeconds;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
    this.missedAcks = missedAcks;
    this.destinationNameFactory = destinationNameFactory;
  }

  public static WeightedReconnectingRSocket newInstance(
      RequestHandlingSocketFactory requestHandlingRSocket,
      DestinationNameFactory destinationNameFactory,
      Function<String, Payload> setupPayloadSupplier,
      BooleanSupplier running,
      Supplier<WeightedClientTransportSupplier> transportSupplier,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      ByteBuf accessToken,
      Quantile lowerQuantile,
      Quantile higherQuantile,
      int inactivityFactor) {
    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            requestHandlingRSocket,
            destinationNameFactory,
            setupPayloadSupplier,
            running,
            transportSupplier,
            keepalive,
            tickPeriodSeconds,
            ackTimeoutSeconds,
            missedAcks,
            accessKey,
            accessToken,
            lowerQuantile,
            higherQuantile,
            inactivityFactor);

    rSocket.resetStatsProcessor();
    rSocket.resetMono();

    rSocket.connect();

    return rSocket;
  }

  private synchronized Duration calculateRetryDuration() {
    long currentTs = System.currentTimeMillis();
    long oldTs = lastConnectionAttemptTs;
    long calculatedDuration = Math.min(attempts, 30);

    if (currentTs - oldTs > CONNECTION_ATTEMPT_RESET_TS) {
      attempts = 0;
    }

    lastConnectionAttemptTs = currentTs;

    attempts++;

    return Duration.ofSeconds(calculatedDuration);
  }

  synchronized void resetStatistics() {
    long now = Clock.now();
    this.stamp = now;
    this.errorStamp = now;
    this.stamp0 = now;
    this.duration = 0L;
    this.pending = 0;

    median.reset();
    interArrivalTime.reset(DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
    pendingStreams.set(0);
    errorPercentage.reset(1.0);
  }

  synchronized void resetStatsProcessor() {
    this.statsProcessor = DirectProcessor.create();
  }

  private DirectProcessor<WeightedRSocket> getStatsProcessor() {
    return statsProcessor;
  }

  private RSocketFactory.ClientRSocketFactory getClientFactory(String destination) {
    RSocketFactory.ClientRSocketFactory connect =
        RSocketFactory.connect().frameDecoder(Frame::retain);

    if (keepalive) {
      connect =
          connect
              .keepAlive()
              .keepAliveTickPeriod(Duration.ofSeconds(tickPeriodSeconds))
              .keepAliveAckTimeout(Duration.ofSeconds(ackTimeoutSeconds))
              .keepAliveMissedAcks(missedAcks);
    } else {
      connect
          .keepAlive()
          .keepAliveAckTimeout(Duration.ofSeconds(0))
          .keepAliveAckTimeout(Duration.ofSeconds(0))
          .keepAliveMissedAcks(missedAcks);
    }

    return connect
        .setupPayload(setupPayloadSupplier.apply(destination))
        .keepAliveAckTimeout(Duration.ofSeconds(0))
        .keepAliveTickPeriod(Duration.ofSeconds(0));
  }

  void connect() {
    synchronized (this) {
      if (connecting) {
        return;
      }

      connecting = true;
    }

    Mono.defer(() -> Mono.delay(calculateRetryDuration()))
        .then(
            Mono.defer(
                () -> {
                  if (onClose.isDisposed()) {
                    return Mono.empty();
                  }

                  WeightedClientTransportSupplier weighedClientTransportSupplier =
                      transportSupplier.get();
                  this.statsProcessor = getStatsProcessor();
                  String destination = destinationNameFactory.get();

                  long start = System.nanoTime();
                  return getClientFactory(destination)
                      .errorConsumer(
                          throwable ->
                              logger.error(
                                  "proteus client received unhandled exception for connection with address "
                                      + weighedClientTransportSupplier
                                          .getSocketAddress()
                                          .toString(),
                                  throwable))
                      .acceptor(r -> requestHandlingRSocket.apply(destination))
                      .transport(weighedClientTransportSupplier.apply(statsProcessor))
                      .start()
                      .doOnNext(
                          _rSocket -> {
                            availability = 1.0;
                            ErrorOnDisconnectRSocket rSocket =
                                new ErrorOnDisconnectRSocket(_rSocket);
                            _rSocket
                                .onClose()
                                .doFinally(
                                    s -> {
                                      long stop = System.nanoTime();

                                      if (Duration.ofNanos(stop - start).getSeconds() < 2) {
                                        logger.warn(
                                            "connection for destionation {} closed in less than 2 seconds - make sure access key {} has a valid access token",
                                            destinationNameFactory.peek(),
                                            accessKey);
                                      }

                                      destinationNameFactory.release(destination);
                                      resetStatsProcessor();
                                      availability = 0.0;
                                      synchronized (WeightedReconnectingRSocket.this) {
                                        connecting = false;
                                      }

                                      rSocket.dispose();

                                      connect();
                                    })
                                .subscribe();
                            setRSocket(rSocket);
                          });
                }))
        .doOnError(t -> logger.error("error trying to broker", t))
        .retry()
        .doFinally(
            s -> {
              if (SignalType.ON_ERROR != s) {
                synchronized (WeightedReconnectingRSocket.this) {
                  connecting = false;
                }
              }
            })
        .subscribe();
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return getRSocket()
        .flatMap(
            source -> {
              long start = start();
              try {
                return source
                    .fireAndForget(payload)
                    .doOnCancel(() -> stop(start))
                    .doOnSuccessOrError(
                        (p, t) -> {
                          long now = stop(start);
                          if (t == null || t instanceof TimeoutException) {
                            record(now - start);
                          }

                          if (t != null) {
                            recordError(0.0);
                          } else {
                            recordError(1.0);
                          }

                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                stop(start);
                recordError(0.0);
                statsProcessor.onNext(WeightedReconnectingRSocket.this);
                return Mono.error(t);
              }
            });
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return getRSocket()
        .flatMap(
            source -> {
              long start = start();
              try {
                return source
                    .requestResponse(payload)
                    .doOnCancel(() -> stop(start))
                    .doOnSuccessOrError(
                        (p, t) -> {
                          long now = stop(start);
                          if (t == null || t instanceof TimeoutException) {
                            record(now - start);
                          }

                          if (t != null) {
                            recordError(0.0);
                          } else {
                            recordError(1.0);
                          }

                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                stop(start);
                recordError(0.0);
                statsProcessor.onNext(WeightedReconnectingRSocket.this);
                return Mono.error(t);
              }
            });
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return getRSocket()
        .flatMapMany(
            source -> {
              try {
                pendingStreams.incrementAndGet();
                return source
                    .requestStream(payload)
                    .doFinally(
                        s -> {
                          pendingStreams.decrementAndGet();
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        })
                    .doOnNext(
                        o -> {
                          recordError(1.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        })
                    .doOnError(
                        t -> {
                          recordError(0.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                recordError(0.0);
                statsProcessor.onNext(WeightedReconnectingRSocket.this);
                return Flux.error(t);
              }
            });
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return getRSocket()
        .flatMapMany(
            source -> {
              try {
                pendingStreams.incrementAndGet();
                return source
                    .requestChannel(payloads)
                    .doFinally(
                        s -> {
                          pendingStreams.decrementAndGet();
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        })
                    .doOnNext(
                        o -> {
                          recordError(1.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        })
                    .doOnError(
                        t -> {
                          recordError(0.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                recordError(0.0);
                statsProcessor.onNext(WeightedReconnectingRSocket.this);
                return Flux.error(t);
              }
            });
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return getRSocket()
        .flatMap(
            source -> {
              long start = start();
              try {
                return source
                    .metadataPush(payload)
                    .doOnCancel(() -> stop(start))
                    .doOnSuccessOrError(
                        (p, t) -> {
                          long now = stop(start);
                          if (t == null || t instanceof TimeoutException) {
                            record(now - start);
                          }

                          if (t != null) {
                            recordError(0.0);
                          } else {
                            recordError(1.0);
                          }
                        });
              } catch (Throwable t) {
                stop(start);
                recordError(0.0);
                statsProcessor.onNext(WeightedReconnectingRSocket.this);
                return Mono.error(t);
              }
            });
  }

  @Override
  public synchronized double predictedLatency() {
    long now = Clock.now();
    long elapsed = Math.max(now - stamp, 1L);

    double weight;
    double prediction = median.estimation();

    if (prediction == 0.0) {
      if (pending == 0) {
        weight = 0.0; // first request
      } else {
        // subsequent requests while we don't have any history
        weight = STARTUP_PENALTY + pending;
      }
    } else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
      // if we did't see any data for a while, we decay the prediction by inserting
      // artificial 0.0 into the median
      median.insert(0.0);
      weight = median.estimation();
    } else {
      double predicted = prediction * pending;
      double instant = instantaneous(now);

      if (predicted < instant) { // NB: (0.0 < 0.0) == false
        weight = instant / pending; // NB: pending never equal 0 here
      } else {
        // we are under the predictions
        weight = prediction;
      }
    }

    return weight;
  }

  private synchronized long instantaneous(long now) {
    return duration + (now - stamp0) * pending;
  }

  private synchronized long start() {
    long now = Clock.now();
    interArrivalTime.insert(now - stamp);
    duration += Math.max(0, now - stamp0) * pending;
    pending += 1;
    stamp = now;
    stamp0 = now;
    return now;
  }

  private synchronized long stop(long timestamp) {
    long now = Clock.now();
    duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
    pending -= 1;
    stamp0 = now;
    return now;
  }

  private synchronized void record(double roundTripTime) {
    median.insert(roundTripTime);
    lowerQuantile.insert(roundTripTime);
    higherQuantile.insert(roundTripTime);
  }

  private synchronized void recordError(double value) {
    errorPercentage.insert(value);
    errorStamp = Clock.now();
  }

  @Override
  public double errorPercentage() {
    return errorPercentage.value();
  }

  @Override
  public double medianLatency() {
    return median.estimation();
  }

  @Override
  public double lowerQuantileLatency() {
    return lowerQuantile.estimation();
  }

  @Override
  public double higherQuantileLatency() {
    return higherQuantile.estimation();
  }

  @Override
  public double interArrivalTime() {
    return interArrivalTime.value();
  }

  @Override
  public int pending() {
    return pending;
  }

  @Override
  public long lastTimeUsedMillis() {
    return stamp0;
  }

  @Override
  public double availability() {
    if (Clock.now() - stamp > tau) {
      recordError(1.0);
    }
    return availability * errorPercentage.value();
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

  void resetMono() {
    if (!onClose.isDisposed()) {
      MonoProcessor<RSocket> _m;
      MonoProcessor<RSocket> old;
      synchronized (this) {
        old = currentSink;
        _m = MonoProcessor.create();
        currentSink = _m;
      }

      if (old != null && !old.isTerminated()) {
        old.onError(new InterruptedException("reset while waiting for new connection"));
      }
    }
  }

  synchronized Mono<RSocket> getRSocket() {
    return currentSink;
  }

  void setRSocket(RSocket rSocket) {
    MonoProcessor<RSocket> _m;
    synchronized (this) {
      _m = currentSink;
      resetStatistics();
    }

    _m.onNext(rSocket);
    _m.onComplete();

    Disposable subscribe =
        onClose
            .doFinally(
                s -> {
                  rSocket.dispose();
                  _m.cancel();
                })
            .subscribe();

    rSocket
        .onClose()
        .doFinally(
            s -> {
              subscribe.dispose();
              resetMono();
            })
        .subscribe();
  }

  public Mono<Void> onReconnect() {
    return currentSink.flatMap(
        rSocket -> rSocket.onClose().then(Mono.defer(() -> currentSink.then())));
  }

  @Override
  public String toString() {
    return "WeightedReconnectingRSocket{"
        + "lowerQuantile="
        + lowerQuantile.estimation()
        + ", higherQuantile="
        + higherQuantile.estimation()
        + ", inactivityFactor="
        + inactivityFactor
        + ", tau="
        + tau
        + ", errorPercentage="
        + errorPercentage.value()
        + ", running="
        + running
        + ", keepalive="
        + keepalive
        + ", tickPeriodSeconds="
        + tickPeriodSeconds
        + ", ackTimeoutSeconds="
        + ackTimeoutSeconds
        + ", missedAcks="
        + missedAcks
        + ", accessKey="
        + accessKey
        + ", accessToken="
        + ByteBufUtil.hexDump(accessToken)
        + ", pending="
        + pending
        + ", errorStamp="
        + errorStamp
        + ", stamp="
        + stamp
        + ", stamp0="
        + stamp0
        + ", duration="
        + duration
        + ", median="
        + median.estimation()
        + ", interArrivalTime="
        + interArrivalTime.value()
        + ", pendingStreams="
        + pendingStreams.get()
        + ", availability="
        + availability
        + '}';
  }

  @FunctionalInterface
  public interface RequestHandlingSocketFactory
      extends Function<String, RSocket> {
    @Override
    RSocket apply(String destination);
  }
}
