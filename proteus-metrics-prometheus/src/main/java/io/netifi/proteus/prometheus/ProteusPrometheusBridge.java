package io.netifi.proteus.prometheus;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netifi.proteus.Proteus;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.prometheus.client.exporter.common.TextFormat;
import io.rsocket.rpc.metrics.om.*;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleConsumer;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Named;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

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
    HttpServer.create()
        .host(bindAddress)
        .port(bindPort)
        .route(routes -> routes.post(metricsUrl, this::handle).get(metricsUrl, this::handle))
        .bind()
        .subscribe();
  }

  private Mono<Void> handle(HttpServerRequest req, HttpServerResponse res) {
    res.status(HttpResponseStatus.OK);
    res.addHeader("Content-Type", TextFormat.CONTENT_TYPE_004);

    return res.sendString(Mono.just(registry.scrape())).then();
  }

  @Override
  public Flux<Skew> streamMetrics(Publisher<MetricsSnapshot> messages, ByteBuf metadata) {
    DirectProcessor<Skew> processor = DirectProcessor.create();

    Disposable subscribe =
        Flux.from(messages)
            .limitRate(256, 32)
            .flatMapIterable(MetricsSnapshot::getMetersList)
            .flatMap(
                meter ->
                    Flux.fromIterable(meter.getMeasureList())
                        .doOnNext(meterMeasurement -> record(meter, meterMeasurement)))
            .doOnComplete(processor::onComplete)
            .doOnError(processor::onError)
            .subscribe();

    Flux.interval(Duration.ofSeconds(metricsSkewInterval))
        .map(l -> Skew.newBuilder().setTimestamp(System.currentTimeMillis()).build())
        .onBackpressureDrop()
        .doFinally(s -> subscribe.dispose())
        .subscribe(processor);

    return processor;
  }

  private void record(io.rsocket.rpc.metrics.om.Meter meter, MeterMeasurement meterMeasurement) {
    try {
      MeterId id = meter.getId();
      Tags tags = mapTags(id.getTagList());
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
                    registry.gauge(generatePrometheusFriendlyName(i), tags, holder);
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
                        Timer timer = registry.timer(generatePrometheusFriendlyName(i), tags);

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
                        Counter counter = registry.counter(generatePrometheusFriendlyName(i), tags);

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
                                new Meter.Id(
                                    generatePrometheusFriendlyName(i),
                                    tags,
                                    baseUnit,
                                    description,
                                    Meter.Type.DISTRIBUTION_SUMMARY),
                                DistributionStatisticConfig.DEFAULT,
                                1000);

                        @Override
                        public void accept(double value) {
                          counter.record(value);
                        }
                      })
              .accept(meterMeasurement.getValue());
          break;
        default:
      }
    } catch (Throwable t) {
      logger.debug("error recording metric for " + meter.getId().getName(), t);
    }
  }

  Tags mapTags(List<MeterTag> tags) {
    Stream<Tag> stream =
        tags.stream().map(meterTag -> Tag.of(meterTag.getKey(), meterTag.getValue()));
    return Tags.of(stream::iterator);
  }

  String generatePrometheusFriendlyName(Meter.Id id) {
    String name = "";
    Optional<Tag> group = findTagByKey(id, "group");

    if (group.isPresent()) {
      name += group.get().getValue();
    }

    Optional<Tag> service = findTagByKey(id, "service");
    if (service.isPresent()) {
      name += "." + service.get().getValue();
    }

    Optional<Tag> method = findTagByKey(id, "method");
    if (method.isPresent()) {
      name += "." + method.get().getValue();
    }

    if (name.isEmpty()) {
      return name;
    } else {
      return name + "." + id.getName();
    }
  }

  private Optional<Tag> findTagByKey(Meter.Id id, String key) {
    return id.getTags().stream().filter(tag -> tag.getKey().equals(key)).findFirst();
  }
}
