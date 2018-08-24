package io.netifi.proteus.metrics;

import org.junit.Ignore;

@Ignore
public class ProteusMeterRegistryTest {
  /*@Test
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

    MetricsExporter exporter = new MetricsExporter(client, registry);
    exporter.run();

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }

    *//* ProteusMeterRegistry registry =
        new ProteusMeterRegistry(ProteusSpectatorConfig.defaultInstance(), client);

    registry.start();

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }*//*
  }

  @Test
  public void testMetricsThroughBroker() throws InterruptedException {
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
      
    Proteus proteus =
        Proteus.builder()
            .group("proteus.metrics.test")
            .accessKey(7685465987873703191L)
            .accessToken("PYYgV9XHSJ/3KqgK5wYjz+73MeA=")
            .accountId(100)
            .minHostsAtStartup(1)
            .poolSize(1)
            .host("localhost")
            .meterRegistry(registry)
            .metricExportFrequencySeconds(5)
            .metricBatchSize(1)
            .build();
    
    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }
  }

  @Test
  public void testMetricsThroughBrokerWithNetifi() throws InterruptedException {
    *//*
    Proteus netifi = Proteus
                        .builder()
                        .group("netifi.metrics.test")
                        .accessKey(7685465987873703191L)
                        .accessToken("PYYgV9XHSJ/3KqgK5wYjz+73MeA=")
                        .accountId(100)
                        .minHostsAtStartup(1)
                        .poolSize(1)
                        .host("localhost")
                        .build();

    ProteusSocket socket = netifi.connect("netifi.metrics").block();

    MetricsSnapshotHandler client = new MetricsSnapshotHandlerClient(socket);

    ProteusMeterRegistry registry =
        new ProteusMeterRegistry(ProteusSpectatorConfig.defaultInstance(), client);

    Counter test = registry.counter("test");

    while (true) {
      test.increment();
    }*//*
  }*/
}
