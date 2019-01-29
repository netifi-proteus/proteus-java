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
