package io.netifi.proteus.metrics;

import com.netflix.spectator.atlas.NetifiAtlasConfig;
import io.micrometer.core.instrument.Counter;
import io.netifi.proteus.metrics.om.*;
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
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Ignore
public class AtlasMeterRegistryTest {
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

    AtlasMeterRegistry registry =
        new AtlasMeterRegistry(NetifiAtlasConfig.defaultInstance(), client);

    registry.start();

    Counter test = registry.counter("test");

    Schedulers.elastic()
        .createWorker()
        .schedulePeriodically(
            () -> {
              test.increment();
            },
            10,
            10,
            TimeUnit.MILLISECONDS);

    Thread.sleep(50_000);
  }
}
