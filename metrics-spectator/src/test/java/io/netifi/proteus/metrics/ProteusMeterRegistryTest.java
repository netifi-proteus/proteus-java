package io.netifi.proteus.metrics;

import com.netflix.spectator.atlas.AtlasConfig;
import io.micrometer.atlas.AtlasMeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.netifi.proteus.Netifi;
import io.netifi.proteus.metrics.om.*;
import io.netifi.proteus.rs.NetifiSocket;
import io.netty.buffer.ByteBuf;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Ignore
public class ProteusMeterRegistryTest {
  @Test
  public void testMetrics() throws InterruptedException {
    MetricsSnapshotHandlerServer snapshotHandlerServer =
        new MetricsSnapshotHandlerServer(
            new MetricsSnapshotHandler() {
              @Override
              public Flux<Skew> streamMetrics(
                  Publisher<MetricsSnapshot> messages, ByteBuf metadata) {

                Flux.from(messages)
                    .doOnNext(
                        metricsSnapshot -> {
                          String s = metricsSnapshot.toString();
                          System.out.println(s);
                        })
                    .subscribe();

                return Flux.interval(Duration.ofSeconds(30))
                    .map(l -> Skew.newBuilder().setTimestamp(System.currentTimeMillis()).build())
                    .onBackpressureLatest();
              }
            });

    RSocketFactory.receive()
        .acceptor((setup, sendingSocket) -> Mono.just(snapshotHandlerServer))
        .transport(TcpServerTransport.create("127.0.0.1", 9800))
        .start()
        .block();

    RSocket block =
        RSocketFactory.connect()
            .transport(TcpClientTransport.create("127.0.0.1", 9800))
            .start()
            .block();

    MetricsSnapshotHandler client = new MetricsSnapshotHandlerClient(block);

    // MeterRegistry registry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
    AtlasMeterRegistry registry =
        new AtlasMeterRegistry(
            new AtlasConfig() {
              @Override
              public String get(String k) {
                return null;
              }

              @Override
              public boolean enabled() {
                return false;
              }
            });

    MeterExporter exporter = new MeterExporter(client, registry);
    exporter.run();

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }

    /* ProteusMeterRegistry registry =
        new ProteusMeterRegistry(ProteusSpectatorConfig.defaultInstance(), client);

    registry.start();

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }*/
  }

  @Test
  public void testMetricsThroughBroker() throws InterruptedException {
    Netifi netifi =
        Netifi.builder()
            .group("netifi.metrics.test")
            .accessKey(7685465987873703191L)
            .accessToken("PYYgV9XHSJ/3KqgK5wYjz+73MeA=")
            .accountId(100)
            .minHostsAtStartup(1)
            .poolSize(1)
            .host("localhost")
            .build();

    NetifiSocket socket = netifi.connect("netifi.metrics").block();

    MetricsSnapshotHandler client = new MetricsSnapshotHandlerClient(socket);
    AtlasMeterRegistry registry =
        new AtlasMeterRegistry(
            new AtlasConfig() {
              @Override
              public String get(String k) {
                return null;
              }

              @Override
              public boolean enabled() {
                return false;
              }
            });

    MeterExporter exporter = new MeterExporter(client, registry);
    exporter.run();

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }
  }

  @Test
  public void testMetricsThroughBrokerWithNetifi() throws InterruptedException {
    /*
    Netifi netifi = Netifi
                        .builder()
                        .group("netifi.metrics.test")
                        .accessKey(7685465987873703191L)
                        .accessToken("PYYgV9XHSJ/3KqgK5wYjz+73MeA=")
                        .accountId(100)
                        .minHostsAtStartup(1)
                        .poolSize(1)
                        .host("localhost")
                        .build();

    NetifiSocket socket = netifi.connect("netifi.metrics").block();

    MetricsSnapshotHandler client = new MetricsSnapshotHandlerClient(socket);

    ProteusMeterRegistry registry =
        new ProteusMeterRegistry(ProteusSpectatorConfig.defaultInstance(), client);

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }*/
  }
}
