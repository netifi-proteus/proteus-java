package io.netifi.proteus.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.netifi.proteus.metrics.om.*;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ProteusMetricsExporter implements Disposable, Runnable {
  private final Logger logger = LoggerFactory.getLogger(ProteusMetricsExporter.class);
  private final MetricsSnapshotHandler handler;
  private final MeterRegistry registry;
  private final Duration exportFrequency;
  private final int batchSize;
  private volatile Disposable disposable;

  public ProteusMetricsExporter(MetricsSnapshotHandler handler, MeterRegistry registry) {
    this(handler, registry, Duration.ofSeconds(10), 1_000);
  }

  public ProteusMetricsExporter(
      MetricsSnapshotHandler handler,
      MeterRegistry registry,
      Duration exportFrequency,
      int batchSize) {
    this.handler = handler;
    this.registry = registry;
    this.exportFrequency = exportFrequency;
    this.batchSize = batchSize;
  }

  private static final String round(double percentile) {
    double roundOff = (double) Math.round(percentile * 10_000.0) / 10_000.0;
    return String.valueOf(roundOff);
  }

  private Flux<MetricsSnapshot> getMetricsSnapshotStream() {
    return Flux.interval(exportFrequency)
        .onBackpressureLatest()
        .concatMap(
            l ->
                Flux.fromIterable(registry.getMeters())
                    .window(batchSize)
                    .flatMap(
                        meters ->
                            meters
                                .groupBy(
                                    meter -> {
                                      if (meter instanceof Timer) {
                                        return true;
                                      } else {
                                        return false;
                                      }
                                    })
                                .flatMap(
                                    grouped -> {
                                      if (grouped.key()) {
                                        return grouped.reduce(
                                            MetricsSnapshot.newBuilder(),
                                            (builder, meter) -> {
                                              Timer timer = (Timer) meter;
                                              List<ProteusMeter> convert = convert(timer);
                                              builder.addAllMeters(convert);
                                              return builder;
                                            });
                                      } else {
                                        return grouped.reduce(
                                            MetricsSnapshot.newBuilder(),
                                            (builder, meter) -> {
                                              ProteusMeter convert = convert(meter);
                                              builder.addMeters(convert);
                                              return builder;
                                            });
                                      }
                                    })
                                .map(MetricsSnapshot.Builder::build)));
  }

  private List<ProteusMeter> convert(Timer timer) {
    List<ProteusMeter> meters = new ArrayList<>();
    HistogramSnapshot snapshot = timer.takeSnapshot();

    Meter.Id id = timer.getId();
    Meter.Type type = id.getType();

    List<MeterTag> meterTags =
        StreamSupport.stream(id.getTags().spliterator(), false)
            .map(tag -> MeterTag.newBuilder().setKey(tag.getKey()).setValue(tag.getValue()).build())
            .collect(Collectors.toList());

    ValueAtPercentile[] valueAtPercentiles = snapshot.percentileValues();
    for (ValueAtPercentile percentile : valueAtPercentiles) {
      ProteusMeter.Builder meterBuilder = ProteusMeter.newBuilder();
      double value = percentile.value(TimeUnit.NANOSECONDS);
      MeterTag tag =
          MeterTag.newBuilder()
              .setKey("percentile")
              .setValue(round(percentile.percentile()))
              .build();
      MeterId.Builder idBuilder = MeterId.newBuilder();
      idBuilder.setName(id.getName());
      idBuilder.addAllTag(meterTags);
      idBuilder.setType(convert(type));
      if (id.getDescription() != null) {
        idBuilder.setDescription(id.getDescription());
      }
      idBuilder.setBaseUnit("nanoseconds");
      idBuilder.addTag(tag);

      meterBuilder.setId(idBuilder);
      meterBuilder.addMeasure(
          MeterMeasurement.newBuilder().setValue(value).setStatistic(MeterStatistic.DURATION));
      ProteusMeter meter = meterBuilder.build();
      meters.add(meter);
    }

    ProteusMeter convert = convert((Meter) timer);
    meters.add(convert);

    return meters;
  }

  private ProteusMeter convert(Meter meter) {
    ProteusMeter.Builder meterBuilder = ProteusMeter.newBuilder();
    MeterId.Builder idBuilder = MeterId.newBuilder();

    Meter.Id id = meter.getId();
    Meter.Type type = id.getType();

    List<MeterTag> meterTags =
        StreamSupport.stream(id.getTags().spliterator(), false)
            .map(tag -> MeterTag.newBuilder().setKey(tag.getKey()).setValue(tag.getValue()).build())
            .collect(Collectors.toList());

    idBuilder.setName(id.getName());
    idBuilder.addAllTag(meterTags);
    idBuilder.setType(convert(type));
    if (id.getDescription() != null) {
      idBuilder.setDescription(id.getDescription());
    }
    if (id.getBaseUnit() != null) {
      idBuilder.setBaseUnit(id.getBaseUnit());
    }

    meterBuilder.setId(idBuilder);

    List<MeterMeasurement> meterMeasurements =
        StreamSupport.stream(meter.measure().spliterator(), false)
            .map(
                measurement ->
                    MeterMeasurement.newBuilder()
                        .setValue(measurement.getValue())
                        .setStatistic(convert(measurement.getStatistic()))
                        .build())
            .collect(Collectors.toList());

    meterBuilder.addAllMeasure(meterMeasurements);

    return meterBuilder.build();
  }

  private MeterType convert(Meter.Type type) {
    switch (type) {
      case GAUGE:
        return MeterType.GAUGE;
      case TIMER:
        return MeterType.TIMER;
      case COUNTER:
        return MeterType.COUNTER;
      case LONG_TASK_TIMER:
        return MeterType.LONG_TASK_TIMER;
      case DISTRIBUTION_SUMMARY:
        return MeterType.DISTRIBUTION_SUMMARY;
      case OTHER:
        return MeterType.OTHER;
      default:
        throw new IllegalStateException("unknown type " + type.name());
    }
  }

  private MeterStatistic convert(Statistic statistic) {
    switch (statistic) {
      case MAX:
        return MeterStatistic.MAX;
      case COUNT:
        return MeterStatistic.COUNT;
      case TOTAL:
        return MeterStatistic.TOTAL;
      case VALUE:
        return MeterStatistic.VALUE;
      case UNKNOWN:
        return MeterStatistic.UNKNOWN;
      case DURATION:
        return MeterStatistic.DURATION;
      case TOTAL_TIME:
        return MeterStatistic.TOTAL_TIME;
      case ACTIVE_TASKS:
        return MeterStatistic.ACTIVE_TASKS;
      default:
        throw new IllegalStateException("unknown type " + statistic.name());
    }
  }

  private void recordClockSkew(long timestamp) {}

  @Override
  public void dispose() {
    Disposable d;
    synchronized (this) {
      d = disposable;
      disposable = null;
    }
    d.dispose();
  }

  @Override
  public boolean isDisposed() {
    if (disposable != null) {
      return false;
    } else {
      return disposable.isDisposed();
    }
  }

  @Override
  public void run() {
    synchronized (this) {
      if (disposable != null) {
        return;
      }
    }

    this.disposable =
        Flux.defer(() -> handler.streamMetrics(getMetricsSnapshotStream(), Unpooled.EMPTY_BUFFER))
            .timeout(
                Duration.ofSeconds(45),
                Mono.error(new TimeoutException("timeout getting clock skew")))
            .doOnNext(skew -> recordClockSkew(skew.getTimestamp()))
            .onErrorResume(
                throwable -> {
                  logger.debug("error streaming data, retrying in 30 seconds", throwable);
                  return Mono.delay(Duration.ofSeconds(30)).then(Mono.error(throwable));
                })
            .retry()
            .subscribe();
  }
}
