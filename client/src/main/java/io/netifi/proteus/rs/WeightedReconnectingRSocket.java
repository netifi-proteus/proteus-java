package io.netifi.proteus.rs;

import io.netifi.proteus.auth.SessionUtil;
import io.netifi.proteus.balancer.transport.ClientTransportSupplierFactory;
import io.netifi.proteus.discovery.DestinationNameFactory;
import io.netifi.proteus.stats.Ewma;
import io.netifi.proteus.stats.Median;
import io.netifi.proteus.stats.Quantile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.*;
import io.rsocket.exceptions.TimeoutException;
import io.rsocket.util.Clock;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * A secure RSocket implementation that contains information about its the error percentage and
 * latency. The RSocket will automatically attempt to reconnect if the it loses it's connection. It
 * selects connections from the {@link ClientTransportSupplierFactory} to determine with connection
 * to use.
 *
 * @see ClientTransportSupplierFactory
 */
public class WeightedReconnectingRSocket implements WeightedRSocket, SecureRSocket {
  private static final Logger logger = LoggerFactory.getLogger(WeightedReconnectingRSocket.class);

  private static final RSocket EMPTY_SOCKET = new AbstractRSocket() {};
  private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;
  private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
      Clock.unit().convert(1L, TimeUnit.SECONDS);
  private final Quantile lowerQuantile;
  private final Quantile higherQuantile;
  private final long inactivityFactor;
  private final long tau;
  private final Ewma errorPercentage;
  private final SessionUtil sessionUtil = SessionUtil.instance();
  private final ReplayProcessor<Mono<RSocket>> source;
  private final MonoProcessor<Void> onClose;
  private final Function<String, Payload> setupPayloadSupplier;
  private final BooleanSupplier running;
  private final boolean keepalive;
  private final long tickPeriodSeconds;
  private final long ackTimeoutSeconds;
  private final int missedAcks;
  private final RSocket requestHandlingRSocket;
  private final long accessKey;
  private final byte[] accessTokenBytes;
  private final ClientTransportSupplierFactory transportFactory;
  private final DestinationNameFactory destinationNameFactory;

  // private RSocketSupplier transportFactory;
  private volatile int pending; // instantaneous rate
  private long errorStamp; // last we got an error
  private long stamp; // last timestamp we sent a request
  private long stamp0; // last timestamp we sent a request or receive a response
  private long duration; // instantaneous cumulative duration
  private Median median;
  private Ewma interArrivalTime;
  private AtomicLong pendingStreams; // number of active streams
  private MonoProcessor<RSocket> currentSink;

  ReplayProcessor<AtomicLong> currentSessionCounter = ReplayProcessor.cacheLast();
  ReplayProcessor<byte[]> currentSessionToken = ReplayProcessor.cacheLast();

  private double availability = 0.0;

  private DirectProcessor<WeightedRSocket> statsProcessor;

  WeightedReconnectingRSocket(
      RSocket requestHandlingRSocket,
      DestinationNameFactory destinationNameFactory,
      Function<String, Payload> setupPayloadSupplier,
      BooleanSupplier running,
      ClientTransportSupplierFactory transportFactory,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      byte[] accessTokenBytes,
      Quantile lowerQuantile,
      Quantile higherQuantile,
      int inactivityFactor) {
    this.transportFactory = transportFactory;
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
    this.source = ReplayProcessor.cacheLast();
    this.setupPayloadSupplier = setupPayloadSupplier;
    this.running = running;
    this.keepalive = keepalive;
    this.accessKey = accessKey;
    this.accessTokenBytes = accessTokenBytes;
    this.tickPeriodSeconds = tickPeriodSeconds;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
    this.missedAcks = missedAcks;
    this.destinationNameFactory = destinationNameFactory;
  }

  public static WeightedReconnectingRSocket newInstance(
      RSocket requestHandlingRSocket,
      DestinationNameFactory destinationNameFactory,
      Function<String, Payload> setupPayloadSupplier,
      BooleanSupplier running,
      ClientTransportSupplierFactory transportFactory,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      byte[] accessTokenBytes,
      Quantile lowerQuantile,
      Quantile higherQuantile,
      int inactivityFactor) {
    WeightedReconnectingRSocket rSocket =
        new WeightedReconnectingRSocket(
            requestHandlingRSocket,
            destinationNameFactory,
            setupPayloadSupplier,
            running,
            transportFactory,
            keepalive,
            tickPeriodSeconds,
            ackTimeoutSeconds,
            missedAcks,
            accessKey,
            accessTokenBytes,
            lowerQuantile,
            higherQuantile,
            inactivityFactor);

    rSocket.resetStatsProcessor();
    rSocket.resetMono();

    rSocket.connect(1).subscribe();
    
    return rSocket;
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

  public SessionUtil getSessionUtil() {
    return sessionUtil;
  }

  public Mono<AtomicLong> getCurrentSessionCounter() {
    return currentSessionCounter.next();
  }

  public Mono<byte[]> getCurrentSessionToken() {
    return currentSessionToken.next();
  }

  private RSocketFactory.ClientRSocketFactory getClientFactory(String destination) {
    RSocketFactory.ClientRSocketFactory connect =
        RSocketFactory.connect().frameDecoder(Frame::retain);

    if (keepalive) {
      connect =
          connect
              .keepAlive()
              .keepAliveAckTimeout(Duration.ofSeconds(tickPeriodSeconds))
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

  Mono<RSocket> connect(int retry) {
    return transportFactory
        .get()
        .flatMap(
            weighedClientTransportSupplier -> {
              if (running.getAsBoolean()) {
                try {

                  this.statsProcessor = getStatsProcessor();

                  String destination = destinationNameFactory.get();
                  return getClientFactory(destination)
                      .errorConsumer(
                          throwable ->
                              logger.error("netifi sdk received unhandled exception", throwable))
                      .acceptor(
                          r ->
                              requestHandlingRSocket == null
                                  ? EMPTY_SOCKET
                                  : requestHandlingRSocket)
                      .transport(weighedClientTransportSupplier.apply(statsProcessor))
                      .start()
                      .doOnNext(
                          rSocket -> {
                            availability = 1.0;
                            rSocket
                                .onClose()
                                .doFinally(
                                    s -> {
                                      destinationNameFactory.release(destination);
                                      resetStatsProcessor();
                                      availability = 0.0;
                                      connect(1).subscribe();
                                    })
                                .subscribe();
                            setRSocket(rSocket);
                          })
                      .onErrorResume(
                          t -> {
                            destinationNameFactory.release(destination);
                            logger.error(t.getMessage(), t);
                            return retryConnection(retry);
                          })
                      .doOnNext(rSocket -> onClose.doFinally(s -> rSocket.dispose()).subscribe());

                } catch (Throwable t) {
                  return retryConnection(retry);
                }
              } else {
                return Mono.empty();
              }
            });
  }

  private Mono<RSocket> retryConnection(int retry) {
    logger.debug("delaying retry {} seconds", retry);
    return Mono.delay(Duration.ofSeconds(retry))
        .then(Mono.defer(() -> connect(Math.min(retry + 1, 10))));
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return getRSocket()
        .flatMap(
            source -> {
              long start = incr();
              try {
                return source
                    .fireAndForget(payload)
                    .doOnCancel(() -> decr(start))
                    .doOnSuccessOrError(
                        (p, t) -> {
                          long now = decr(start);
                          if (t == null || t instanceof TimeoutException) {
                            observe(now - start);
                          }

                          if (t != null) {
                            updateErrorPercentage(0.0);
                          } else {
                            updateErrorPercentage(1.0);
                          }

                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                decr(start);
                updateErrorPercentage(0.0);
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
              long start = incr();
              try {
                return source
                    .requestResponse(payload)
                    .doOnCancel(() -> decr(start))
                    .doOnSuccessOrError(
                        (p, t) -> {
                          long now = decr(start);
                          if (t == null || t instanceof TimeoutException) {
                            observe(now - start);
                          }

                          if (t != null) {
                            updateErrorPercentage(0.0);
                          } else {
                            updateErrorPercentage(1.0);
                          }

                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                decr(start);
                updateErrorPercentage(0.0);
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
                          updateErrorPercentage(1.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        })
                    .doOnError(
                        t -> {
                          updateErrorPercentage(0.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                updateErrorPercentage(0.0);
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
                          updateErrorPercentage(1.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        })
                    .doOnError(
                        t -> {
                          updateErrorPercentage(0.0);
                          statsProcessor.onNext(WeightedReconnectingRSocket.this);
                        });
              } catch (Throwable t) {
                updateErrorPercentage(0.0);
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
              long start = incr();
              try {
                return source
                    .metadataPush(payload)
                    .doOnCancel(() -> decr(start))
                    .doOnSuccessOrError(
                        (p, t) -> {
                          long now = decr(start);
                          if (t == null || t instanceof TimeoutException) {
                            observe(now - start);
                          }

                          if (t != null) {
                            updateErrorPercentage(0.0);
                          } else {
                            updateErrorPercentage(1.0);
                          }
                        });
              } catch (Throwable t) {
                decr(start);
                updateErrorPercentage(0.0);
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

  private synchronized long incr() {
    long now = Clock.now();
    interArrivalTime.insert(now - stamp);
    duration += Math.max(0, now - stamp0) * pending;
    pending += 1;
    stamp = now;
    stamp0 = now;
    return now;
  }

  private synchronized long decr(long timestamp) {
    long now = Clock.now();
    duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
    pending -= 1;
    stamp0 = now;
    return now;
  }

  private synchronized void observe(double rtt) {
    median.insert(rtt);
    lowerQuantile.insert(rtt);
    higherQuantile.insert(rtt);
  }

  private synchronized void updateErrorPercentage(double value) {
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
      updateErrorPercentage(1.0);
    }
    return availability * errorPercentage.value();
  }

  @Override
  public void dispose() {
    source.dispose();
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
    MonoProcessor<RSocket> _m;
    synchronized (this) {
      _m = MonoProcessor.create();
      currentSink = _m;
    }

    source.onNext(_m);
  }

  Mono<RSocket> getRSocket() {
    return source.next().flatMap(Function.identity());
  }

  void setRSocket(RSocket rSocket) {
    byte[] sessionToken;
    synchronized (this) {
      long count = sessionUtil.getThirtySecondsStepsFromEpoch();
      currentSessionCounter.onNext(new AtomicLong(count));
      ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
      try {
        byteBuf.writeLong(accessKey);
        sessionToken = sessionUtil.generateSessionToken(accessTokenBytes, byteBuf, count);
      } finally {
        if (byteBuf.refCnt() > 0) {
          byteBuf.release();
        }
      }

      resetStatistics();
    }

    MonoProcessor<RSocket> _m;
    synchronized (this) {
      _m = currentSink;
    }

    currentSessionToken.onNext(sessionToken);
    _m.onNext(rSocket);
    _m.onComplete();

    Disposable subscribe = onClose.doFinally(s -> rSocket.dispose()).subscribe();

    rSocket
        .onClose()
        .doFinally(
            s -> {
              subscribe.dispose();
              resetMono();
            })
        .subscribe();
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
        + ", accessTokenBytes="
        + Arrays.toString(accessTokenBytes)
        + ", transportFactory="
        + transportFactory.toString()
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
}
