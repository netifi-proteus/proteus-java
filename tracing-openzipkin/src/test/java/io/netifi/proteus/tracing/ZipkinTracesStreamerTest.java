package io.netifi.proteus.tracing;

import io.netty.channel.ChannelOption;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.resources.PoolResources;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class ZipkinTracesStreamerTest {

  private static final int COUNT = 42;
  private TracesStreamer tracesStreamer;

  @Before
  public void setUp() {
    tracesStreamer = new TracesStreamer(zipkinSource(COUNT));
  }

  @Test
  public void zipkinServerTracesStreaming() {
    List<Trace> traces = tracesStreamer
        .streamTraces(42)
        .collectList()
        .block(Duration.ofSeconds(10));
    Assert.assertEquals(COUNT, traces.size());
  }

  private Flux<InputStream> zipkinSource(int count) {
    return Mono.fromCallable(() -> {
      try (InputStream trace =
               getClass()
                   .getClassLoader()
                   .getResourceAsStream("zipkin_trace.json")) {
        Charset utf8 = StandardCharsets.UTF_8;
        try (java.util.Scanner s = new Scanner(trace, utf8.name())) {
          return s.useDelimiter("\\A").hasNext() ? s.next() : "";
        }
      }
    }).flatMapMany(trace ->
        Flux.range(1, count)
            .map(v -> trace)
            .map(t -> new ByteArrayInputStream(t.getBytes(StandardCharsets.UTF_8))));
  }

  @Ignore("requires local zipkin server")
  @Test
  public void streamerIntegrationTest() {
    TracesStreamer streamer = new TracesStreamer("/api/v2/traces",
        client());
    Flux<Trace> traces = streamer.streamTraces((int) TimeUnit.DAYS.toSeconds(10));
    List<Trace> tracesList = traces.collectList().block();
    Assert.assertFalse(tracesList.isEmpty());
  }

  private Mono<HttpClient> client() {
    return Mono.just(HttpClient.builder()
        .options(
            builder ->
                builder
                    .compression(true)
                    .poolResources(PoolResources.fixed("proteusZipkinBridge"))
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30_000)
                    .host("127.0.0.1")
                    .port(9411))
        .build());
  }
}
