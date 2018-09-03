package io.netifi.proteus.tracing;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;

public class ZipkinTracesStreamerTest {

  private TracesStreamer tracesStreamer;

  @Before
  public void setUp() {
    tracesStreamer = new TracesStreamer(zipkinSource());
  }

  @Test
  public void zipkinServerTracesStreaming() {
    List<Trace> traces = tracesStreamer
        .streamTraces(42)
        .collectList()
        .block(Duration.ofSeconds(10));
    Assert.assertFalse(traces.isEmpty());
  }

  private Mono<InputStream> zipkinSource() {
    return Mono.fromCallable(() ->
        getClass().getClassLoader().getResourceAsStream("zipkin.json"));
  }
}
