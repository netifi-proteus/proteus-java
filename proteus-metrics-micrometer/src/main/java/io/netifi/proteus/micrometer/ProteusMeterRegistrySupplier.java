package io.netifi.proteus.micrometer;

import com.google.common.collect.Streams;
import com.netflix.spectator.atlas.AtlasConfig;
import io.micrometer.atlas.AtlasMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.rsocket.ProteusSocket;
import io.netifi.proteus.tags.DefaultTags;
import io.rsocket.rpc.metrics.MetricsExporter;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandlerClient;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Named;

@Named("ProteusMeterRegistrySupplier")
public class ProteusMeterRegistrySupplier implements Supplier<MeterRegistry> {
  private final MeterRegistry registry;

  @Inject
  public ProteusMeterRegistrySupplier(
      Proteus proteus,
      Optional<String> metricsGroup,
      Optional<Long> stepInMillis,
      Optional<Boolean> export) {
    Objects.requireNonNull(proteus, "must provide a Proteus instance");
    DefaultTags toTags = new DefaultTags();
    toTags.add("group", metricsGroup.orElse("com.netifi.proteus.metrics"));
    ProteusSocket proteusSocket = proteus.unicast(toTags);

    MetricsSnapshotHandlerClient client = new MetricsSnapshotHandlerClient(proteusSocket);

    long millis = stepInMillis.orElse(10_000L);
    Duration stepDuration = Duration.ofMillis(millis);

    this.registry =
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

              @Override
              public Duration step() {
                return stepDuration;
              }
            });

    Tags commonTags =
        Streams.stream(proteus.getTags())
            .reduce(
                Tags.empty(),
                (tags, entry) -> tags.and(entry.getKey().toString(), entry.getValue().toString()),
                Tags::and)
            .and("accessKey", String.valueOf(proteus.getAccesskey()));

    registry.config().commonTags(commonTags);

    new ProteusOperatingSystemMetrics(registry, Collections.emptyList());

    if (export.orElse(true)) {
      MetricsExporter exporter = new MetricsExporter(client, registry, stepDuration, 1024);
      exporter.run();

      proteus.onClose().doFinally(s -> exporter.dispose()).subscribe();
    }
  }

  @Override
  public MeterRegistry get() {
    return registry;
  }
}
