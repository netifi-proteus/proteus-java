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
package io.netifi.proteus.tracing;

import brave.Tracing;
import brave.opentracing.BraveTracer;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.rsocket.ProteusSocket;
import io.opentracing.Tracer;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Named;

@Named("ProteusTracerSupplier")
public class ProteusTracerSupplier implements Supplier<Tracer> {
  private final Tracer tracer;

  @Inject
  public ProteusTracerSupplier(Proteus proteus, Optional<String> tracingGroup) {
    ProteusSocket proteusSocket = proteus.group(tracingGroup.orElse("com.netifi.proteus.tracing"));

    ProteusTracingServiceClient client = new ProteusTracingServiceClient(proteusSocket);
    ProteusReporter reporter =
        new ProteusReporter(client, proteus.getGroupName(), proteus.getTags());

    Tracing tracing = Tracing.newBuilder().spanReporter(reporter).build();

    tracer = BraveTracer.create(tracing);
  }

  @Override
  public Tracer get() {
    return tracer;
  }
}
