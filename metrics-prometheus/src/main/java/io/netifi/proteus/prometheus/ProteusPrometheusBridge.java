package io.netifi.proteus.prometheus;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.metrics.om.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.prometheus.client.exporter.common.TextFormat;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleConsumer;
import java.util.stream.Collectors;

@Named("ProteusPrometheusBridge")
public class ProteusPrometheusBridge implements MetricsSnapshotHandler {
  private static final Logger logger = LoggerFactory.getLogger(ProteusPrometheusBridge.class);
  private final long metricsSkewInterval;
  private final PrometheusMeterRegistry registry;
  private final String bindAddress;
  private final int bindPort;
  private final String metricsUrl;
  ConcurrentHashMap<Meter.Id, DoubleConsumer> consumers = new ConcurrentHashMap<>();

  @Inject
  public ProteusPrometheusBridge(
      Optional<Long> metricsSkewInterval,
      PrometheusMeterRegistry registry,
      Optional<String> bindAddress,
      Optional<Integer> bindPort,
      Optional<String> metricsUrl) {
    this.metricsSkewInterval = metricsSkewInterval.orElse(10_000L);
    this.registry = registry;
    this.bindAddress = bindAddress.orElse("0.0.0.0");
    this.bindPort = bindPort.orElse(8888);
    this.metricsUrl = metricsUrl.orElse("/metrics");

    init();
  }

  public static void main(String... args) {
    logger.info("Starting Stand-alone Proteus Prometheus Bridge");

    String group = System.getProperty("netifi.metricsGroup", "com.netifi.proteus.metrics");
    String brokerHost = System.getProperty("netifi.proteus.host", "localhost");
    int brokerPort = Integer.getInteger("netifi.proteus.port", 8001);
    String bindAddress = System.getProperty("netifi.proteus.metricsBindAddress");
    Integer bindPort = Integer.getInteger("netifi.proteus.metricsBindPort");
    String metricsUrl = System.getProperty("netifi.proteus.metricsUrl");
    long accessKey = Long.getLong("netifi.proteus.accessKey", 3855261330795754807L);
    String accessToken =
        System.getProperty("netifi.authentication.accessToken", "kTBDVtfRBO4tHOnZzSyY5ym2kfY");

    logger.info("group - {}", group);
    logger.info("broker host - {}", brokerHost);
    logger.info("broker port - {}", brokerPort);
    logger.info("access key - {}", accessKey);

    Proteus proteus =
        Proteus.builder()
            .accessKey(accessKey)
            .accessToken(accessToken)
            .group(group)
            .host(brokerHost)
            .port(brokerPort)
            .destination("standalonePrometheusBridge")
            .build();

    proteus.addService(
        new MetricsSnapshotHandlerServer(
            new ProteusPrometheusBridge(
                Optional.empty(),
                new PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
                Optional.ofNullable(bindAddress),
                Optional.ofNullable(bindPort),
                Optional.ofNullable(metricsUrl)),
            Optional.empty(),
            Optional.empty()));

    proteus.onClose().block();
  }

  private void init() {
    HttpServer.create(bindAddress, bindPort)
        .newRouter(routes -> routes.post(metricsUrl, this::handle).get(metricsUrl, this::handle))
        .subscribe();
  }

  private Mono<Void> handle(HttpServerRequest req, HttpServerResponse res) {
    res.status(HttpResponseStatus.OK);
    res.addHeader("Content-Type", TextFormat.CONTENT_TYPE_004);

    return res.sendString(Mono.just(registry.scrape())).then();
  }

  @Override
  public Flux<Skew> streamMetrics(Publisher<MetricsSnapshot> messages, ByteBuf metadata) {
    AtomicBoolean first = new AtomicBoolean();
    Disposable subscribe =
        Flux.from(messages)
            .log()
            .limitRate(256, 32)
            .flatMapIterable(MetricsSnapshot::getMetersList)
            .flatMap(
                meter ->
                    Flux.fromIterable(meter.getMeasureList())
                        .doOnNext(meterMeasurement -> record(meter, meterMeasurement, first)))
            .subscribe();

    return Flux.just(Skew.newBuilder().setTimestamp(System.currentTimeMillis()).build());
  }

  private void record(ProteusMeter meter, MeterMeasurement meterMeasurement, AtomicBoolean first) {
    MeterId id = meter.getId();
    Iterable<Tag> tags = mapTags(id.getTagList());
    String name = id.getName();
    MeterType type = meter.getId().getType();
    String baseUnit = id.getBaseUnit();
    String description = id.getDescription();
    
    switch (type) {
      case GAUGE:
        consumers
            .computeIfAbsent(
                new Meter.Id(name, tags, baseUnit, description, Meter.Type.GAUGE),
                i -> {
                  AtomicDouble holder = new AtomicDouble();
                  registry.gauge(name, tags, holder);
                  return holder::set;
                })
            .accept(meterMeasurement.getValue());
        break;
      case LONG_TASK_TIMER:
      case TIMER:
        consumers
            .computeIfAbsent(
                new Meter.Id(name, tags, baseUnit, description, Meter.Type.TIMER),
                i ->
                    new DoubleConsumer() {
                      Timer timer = registry.timer(name, tags);

                      @Override
                      public void accept(double value) {
                        timer.record(Duration.ofNanos((long) value));
                      }
                    })
            .accept(meterMeasurement.getValue());
        break;
      case COUNTER:
        consumers
            .computeIfAbsent(
                new Meter.Id(name, tags, baseUnit, description, Meter.Type.COUNTER),
                i ->
                    new DoubleConsumer() {
                      Counter counter = registry.counter(name, tags);

                      @Override
                      public void accept(double value) {
                        counter.increment(value);
                      }
                    })
            .accept(meterMeasurement.getValue());
        break;
      case DISTRIBUTION_SUMMARY:
        consumers
            .computeIfAbsent(
                new Meter.Id(name, tags, baseUnit, description, Meter.Type.DISTRIBUTION_SUMMARY),
                i ->
                    new DoubleConsumer() {
                      DistributionSummary counter =
                          registry.newDistributionSummary(
                              i, DistributionStatisticConfig.DEFAULT, 1000);

                      @Override
                      public void accept(double value) {
                        counter.record(value);
                      }
                    })
            .accept(meterMeasurement.getValue());
        break;
      default:
    }
  }

  List<Tag> mapTags(List<MeterTag> tags) {
    return tags.stream()
        .map(meterTag -> Tag.of(meterTag.getKey(), meterTag.getValue()))
        .collect(Collectors.toList());
  }
}
