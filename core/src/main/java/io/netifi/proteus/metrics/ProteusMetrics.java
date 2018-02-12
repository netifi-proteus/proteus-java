package io.netifi.proteus.metrics;

import io.micrometer.core.instrument.*;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Operators;

public class ProteusMetrics {
  ProteusMetrics() {}

  public static <T> Function<? super Publisher<T>, ? extends Publisher<T>> timed(
      MeterRegistry registry, String name, String... keyValues) {
    return timed(registry, name, Tags.of(keyValues));
  }

  public static <T> Function<? super Publisher<T>, ? extends Publisher<T>> timed(
      MeterRegistry registry, String name, Iterable<Tag> tags) {
    Counter success =
        Counter.builder(name + ".request").tags("status", "success").tags(tags).register(registry);
    Counter error =
        Counter.builder(name + ".request").tags("status", "error").tags(tags).register(registry);
    Counter cancelled =
        Counter.builder(name + ".request")
            .tags("status", "cancelled")
            .tags(tags)
            .register(registry);
    Timer timer =
        Timer.builder(name + ".latency").publishPercentileHistogram().tags(tags).register(registry);

    return Operators.lift(
        (scannable, subscriber) ->
            new ProteusMetricsSubscriber<>(subscriber, success, error, cancelled, timer));
  }
}
