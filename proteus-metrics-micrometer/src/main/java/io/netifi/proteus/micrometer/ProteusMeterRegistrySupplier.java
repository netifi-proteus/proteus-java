/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.netifi.proteus.micrometer;

import com.netflix.spectator.atlas.AtlasConfig;
import io.micrometer.atlas.AtlasMeterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.rsocket.ProteusSocket;
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
    ProteusSocket proteusSocket = proteus.group(metricsGroup.orElse("com.netifi.proteus.metrics"));

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

    registry.config().commonTags(proteus.getTags());

    new ProteusOperatingSystemMetrics(registry, Collections.EMPTY_LIST);

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
