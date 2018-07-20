package io.netifi.proteus.tracing;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netifi.proteus.Proteus;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import zipkin2.proto3.Span;

import java.util.Optional;

public class ProteusZipkinHttpBridge implements ProteusTracingService {
  private static final Logger logger = LoggerFactory.getLogger(ProteusZipkinHttpBridge.class);

  private final String host;

  private final int port;

  private final String zipkinUrl;

  private HttpClient httpClient;

  public ProteusZipkinHttpBridge(String host, int port, String zipkinUrl) {
    this.zipkinUrl = zipkinUrl;
    this.host = host;
    this.port = port;
    this.httpClient =
        HttpClient.builder()
            .options(builder -> builder.compression(true).host(host).port(port))
            .build();
  }

  public ProteusZipkinHttpBridge(String host, int port) {
    this(host, port, "/api/v2/spans");
  }

  public static void main(String... args) {
    String group = System.getProperty("netifi.tracingGroup", "com.netifi.proteus.tracing");
    String brokerHost = System.getProperty("netifi.proteus.host", "localhost");
    int brokerPort = Integer.getInteger("netifi.proteus.port", 8001);
    String zipkinHost = System.getProperty("netifi.proteus.zipkinHost", "localhost");
    int zipkinPort = Integer.getInteger("netifi.proteus.zipkinPort", 9411);
    long accessKey = Long.getLong("netifi.proteus.accessKey", 3855261330795754807L);
    String accessToken =
        System.getProperty("netifi.authentication.accessToken", "kTBDVtfRBO4tHOnZzSyY5ym2kfY");

    Proteus proteus =
        Proteus.builder()
            .accessKey(accessKey)
            .accessToken(accessToken)
            .group(group)
            .host(brokerHost)
            .port(brokerPort)
            .build();

    proteus.addService(
        new ProteusTracingServiceServer(
            new ProteusZipkinHttpBridge(zipkinHost, zipkinPort),
            Optional.empty(),
            Optional.empty()));

    proteus.onClose().block();
  }

  @Override
  public Mono<Ack> streamSpans(Publisher<Span> messages, ByteBuf metadata) {
    return Flux.from(messages)
        .flatMap(
            span -> {
              try {
                String json = "[" + JsonFormat.printer().print(span) + "]";
                if (logger.isTraceEnabled()) {
                  logger.trace(json);
                }
                return httpClient.post(
                    zipkinUrl,
                    request -> {
                      request.addHeader("Content-Type", "application/json");
                      return request.sendString(Mono.just(json));
                    });

              } catch (InvalidProtocolBufferException e) {
                throw Exceptions.propagate(e);
              }
            },
            8)
        .then(Mono.just(Ack.getDefaultInstance()));
  }
}
