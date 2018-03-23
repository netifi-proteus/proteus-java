package com.netflix.spectator.atlas;

import com.netflix.spectator.api.*;
import io.netifi.proteus.metrics.om.*;
import io.netty.buffer.Unpooled;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Registry for reporting metrics to Atlas. */
public final class NetifiAtlasRegistry extends AbstractRegistry {

  private static final String CLOCK_SKEW_TIMER = "spectator.atlas.clockSkew";

  private final Clock clock;

  private final boolean enabled;
  private final Duration step;
  private final long stepMillis;
  private final long meterTTL;
  private final int batchSize;

  private final Duration configRefreshFrequency;
  private final Map<String, String> commonTags;

  private Disposable disposable;
  private MetricsSnapshotHandler handler;

  /** Create a new instance. */
  public NetifiAtlasRegistry(
      Clock clock, NetifiAtlasConfig config, MetricsSnapshotHandler handler) {
    super(new StepClock(clock, config.step().toMillis()), config);
    this.clock = clock;

    this.handler = handler;

    this.batchSize = config.batchSize();
    this.enabled = config.enabled();
    this.step = config.step();
    this.stepMillis = step.toMillis();
    this.meterTTL = config.meterTTL().toMillis();
    this.configRefreshFrequency = config.configRefreshFrequency();
    this.commonTags = new TreeMap<>(config.commonTags());
  }

  /** Start the scheduler to collect metrics data. */
  public void start() {
    synchronized (this) {
      if (disposable != null) {
        logger.warn("registry already started, ignoring duplicate request");
        return;
      }

      logger.info("started collecting metrics every {}", step);
      logger.info("common tags: {}", commonTags);
    }

    disposable =
        handler
            .streamMetrics(getMetricsSnapshotStream(), Unpooled.EMPTY_BUFFER)
            .doOnNext(skew -> recordClockSkew(skew.getTimestamp()))
            .onErrorResume(
                throwable -> {
                  logger.error("error streaming data, retrying in 30 seconds", throwable);
                  return Mono.delay(Duration.ofSeconds(30)).then(Mono.error(throwable));
                })
            .retry()
            .subscribe();
  }

  private Flux<MetricsSnapshot> getMetricsSnapshotStream() {
    return Flux.interval(configRefreshFrequency)
        .onBackpressureLatest()
        .concatMap(
            l ->
                Flux.fromStream(stream())
                    .flatMapIterable(Meter::measure)
                    .window(batchSize)
                    .flatMap(
                        meters ->
                            meters
                                .reduce(
                                    MetricsSnapshot.newBuilder().putAllTags(commonTags),
                                    (builder, measurement) -> {
                                      MeterMeasurement converted = convert(measurement);
                                      builder.addMetrics(converted);
                                      return builder;
                                    })
                                .map(MetricsSnapshot.Builder::build)));
  }

  private MeterMeasurement convert(Measurement measurement) {
    Id id = measurement.id();
    long timestamp = measurement.timestamp();
    double value = measurement.value();

    return MeterMeasurement.newBuilder()
        .setId(convert(id))
        .setTimestamp(timestamp)
        .setValue(value)
        .build();
  }

  private MeterId convert(Id id) {
    String name = id.name();
    Iterable<Tag> tags = id.tags();

    MeterId.Builder builder = MeterId.newBuilder();

    builder.setName(name);

    for (Tag t : tags) {
      MeterTag tag = MeterTag.newBuilder().setKey(t.key()).setValue(t.value()).build();

      builder.addTag(tag);
    }

    return builder.build();
  }

  /** Stop the scheduler reporting Atlas data. */
  public synchronized void stop() {
    try {
      if (disposable != null) {
        disposable.dispose();
        logger.info("stopped collecting metrics every {}", step);
      } else {
        logger.warn("registry stopped, but was never started");
      }
    } finally {
      disposable = null;
    }
  }

  /**
   * Record the difference between the date response time and the local time on the server. This is
   * used to get a rough idea of the amount of skew in the environment. Ideally it should be fairly
   * small. The date header will only have seconds so we expect to regularly have differences of up
   * to 1 second. Note, that it is a rough estimate and could be elevated because of unrelated
   * problems like GC or network delays.
   */
  private void recordClockSkew(long responseTimestamp) {
    if (responseTimestamp == 0L) {
      logger.debug("no date timestamp on response, cannot record skew");
    } else {
      final long delta = clock.wallTime() - responseTimestamp;
      if (delta >= 0L) {
        // Local clock is running fast compared to the server. Note this should also be the
        // common case for if the clocks are in sync as there will be some delay for the server
        // response to reach this node.
        timer(CLOCK_SKEW_TIMER, "id", "fast").record(delta, TimeUnit.MILLISECONDS);
      } else {
        // Local clock is running slow compared to the server. This means the response timestamp
        // appears to be after the current time on this node. The timer will ignore negative
        // values so we negate and record it with a different id.
        timer(CLOCK_SKEW_TIMER, "id", "slow").record(-delta, TimeUnit.MILLISECONDS);
      }
      logger.debug("clock skew between client and server: {}ms", delta);
    }
  }

  @Override
  protected Counter newCounter(Id id) {
    return new AtlasCounter(id, clock, meterTTL, stepMillis);
  }

  @Override
  protected DistributionSummary newDistributionSummary(Id id) {
    return new AtlasDistributionSummary(id, clock, meterTTL, stepMillis);
  }

  @Override
  protected Timer newTimer(Id id) {
    return new AtlasTimer(id, clock, meterTTL, stepMillis);
  }

  @Override
  protected Gauge newGauge(Id id) {
    // Be sure to get StepClock so the measurements will have step aligned
    // timestamps.
    return new AtlasGauge(id, clock(), meterTTL);
  }
}
