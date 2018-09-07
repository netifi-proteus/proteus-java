package io.netifi.proteus.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

public class ProteusOperatingSystemMetrics {
  public ProteusOperatingSystemMetrics(MeterRegistry registry, Iterable<Tag> tags) {
    new JvmMemoryMetrics(tags).bindTo(registry);
    new JvmGcMetrics(tags).bindTo(registry);
    new JvmThreadMetrics(tags).bindTo(registry);
    new ClassLoaderMetrics(tags).bindTo(registry);
    new ProcessorMetrics(tags).bindTo(registry);
    new UptimeMetrics(tags).bindTo(registry);
    new FileDescriptorMetrics(tags).bindTo(registry);
  }
}
