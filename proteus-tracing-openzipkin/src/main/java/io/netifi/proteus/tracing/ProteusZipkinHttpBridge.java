package io.netifi.proteus.tracing;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netifi.proteus.Proteus;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import java.time.Duration;
import java.util.Optional;
import java.util.StringJoiner;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.PoolResources;
import zipkin2.proto3.Span;

public class ProteusZipkinHttpBridge implements ProteusTracingService {
  private static final Logger logger = LoggerFactory.getLogger(ProteusZipkinHttpBridge.class);

  private static final String DEFAULT_ZIPKIN_SPANS_URL = "/api/v2/spans";
  private static final Duration DEFAULT_TRACE_RETENTION = Duration.ofSeconds(60);

  private final String host;
  private final int port;
  private final String zipkinSpansUrl;
  private final Duration retentionPeriod;
  private final ReplayProcessor<Span> spansProcessor;
  private HttpClient httpClient;

  public ProteusZipkinHttpBridge(String host, int port, String zipkinSpansUrl) {
    this(host, port, zipkinSpansUrl, DEFAULT_TRACE_RETENTION);
  }

  public ProteusZipkinHttpBridge(String host, int port) {
    this(host, port, DEFAULT_ZIPKIN_SPANS_URL, DEFAULT_TRACE_RETENTION);
  }

  public ProteusZipkinHttpBridge(
      String host, int port, String zipkinSpansUrl, Duration retentionPeriod) {
    this.host = host;
    this.port = port;
    this.zipkinSpansUrl = zipkinSpansUrl;
    this.retentionPeriod = retentionPeriod;
    this.spansProcessor = ReplayProcessor.createTimeout(this.retentionPeriod);
  }

  public static void main(String... args) {
    logger.info("Starting Stand-alone Proteus Zipkin HTTP Bridge");

    String group = System.getProperty("netifi.tracingGroup", "com.netifi.proteus.tracing");
    String brokerHost = System.getProperty("netifi.proteus.host", "localhost");
    int brokerPort = Integer.getInteger("netifi.proteus.port", 8001);
    String zipkinHost = System.getProperty("netifi.proteus.zipkinHost", "localhost");
    int zipkinPort = Integer.getInteger("netifi.proteus.zipkinPort", 9411);
    String zipkinSpansUrl =
        System.getProperty("netifi.proteus.zipkinSpansUrl", DEFAULT_ZIPKIN_SPANS_URL);
    Duration traceRetentionPeriod =
        Duration.ofSeconds(Integer.getInteger("netifi.proteus.traceRetentionPeriod", 60));
    long accessKey = Long.getLong("netifi.proteus.accessKey", 3855261330795754807L);
    String accessToken =
        System.getProperty("netifi.authentication.accessToken", "kTBDVtfRBO4tHOnZzSyY5ym2kfY");

    logger.info("group - {}", group);
    logger.info("broker host - {}", brokerHost);
    logger.info("broker port - {}", brokerPort);
    logger.info("zipkin host - {}", zipkinHost);
    logger.info("zipkin port - {}", zipkinPort);
    logger.info("zipkin spans url - {}", zipkinSpansUrl);
    logger.info("traces retention period - {}", traceRetentionPeriod);
    logger.info("access key - {}", accessKey);

    Proteus proteus =
        Proteus.builder()
            .accessKey(accessKey)
            .accessToken(accessToken)
            .group(group)
            .host(brokerHost)
            .port(brokerPort)
            .destination("standaloneZipkinBridge")
            .build();

    proteus.addService(
        new ProteusTracingServiceServer(
            new ProteusZipkinHttpBridge(
                zipkinHost, zipkinPort, zipkinSpansUrl, traceRetentionPeriod),
            Optional.empty(),
            Optional.empty()));

    proteus.onClose().block();
  }

  private synchronized HttpClient getClient() {
    if (httpClient == null) {
      this.httpClient =
          HttpClient.builder()
              .options(
                  builder ->
                      builder
                          .compression(true)
                          .poolResources(PoolResources.fixed("proteusZipkinBridge"))
                          .option(ChannelOption.SO_KEEPALIVE, true)
                          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30_000)
                          .host(host)
                          .port(port))
              .build();
    }
    return httpClient;
  }

  private synchronized void resetHttpClient() {
    this.httpClient = null;
  }

  @Override
  public Mono<Ack> streamSpans(Publisher<Span> messages, ByteBuf metadata) {
    return Flux.from(messages)
        .map(
            span -> {
              // Retain span for replay
              spansProcessor.onNext(span);

              try {
                String json = JsonFormat.printer().print(span);
                if (logger.isTraceEnabled()) {
                  logger.trace("receiving tracing data {}", json);
                }
                return json;
              } catch (InvalidProtocolBufferException e) {
                throw Exceptions.propagate(e);
              }
            })
        .windowTimeout(128, Duration.ofMillis(1000))
        .map(
            strings ->
                strings
                    .reduce(new StringJoiner(","), StringJoiner::add)
                    .map(stringJoiner -> "[" + stringJoiner.toString() + "]"))
        .onBackpressureBuffer(1 << 16)
        .flatMap(stringMono -> stringMono)
        .concatMap(
            spans ->
                getClient()
                    .post(
                        zipkinSpansUrl,
                        request -> {
                          request.addHeader("Content-Type", "application/json");
                          return request.sendString(Mono.just(spans));
                        })
                    .timeout(Duration.ofSeconds(30))
                    .doOnError(throwable -> resetHttpClient()),
            8)
        .doOnError(
            throwable ->
                logger.error(
                    "error sending data to tracing data to url " + zipkinSpansUrl, throwable))
        .then(Mono.never());
  }

  @Override
  public Flux<Trace> streamTraces(TracesRequest message, ByteBuf metadata) {
    Duration skipDuration = retentionPeriod.minus(Duration.ofSeconds(message.getLookbackSeconds()));
    // This isn't working for the moment - WIP
    // if (skipDuration.isNegative()) {
    //    } else {
    //      traces = spansProcessor.skip(skipDuration);
    //    }
    return spansProcessor
        .filter(p -> p != null)
        .window(Duration.ofSeconds(1))
        .flatMap(spans -> spans.collectList())
        .filter(spans -> spans.size() > 0)
        .map(spans -> Trace.newBuilder().addAllSpans(spans).build())
        .onBackpressureDrop();
  }
}
