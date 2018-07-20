package io.netifi.proteus.tracing;

import brave.Tracing;
import brave.opentracing.BraveTracer;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.rsocket.ProteusSocket;
import io.opentracing.Tracer;

import java.util.Optional;
import java.util.function.Supplier;

public class ProteusTracerSupplier implements Supplier<Tracer> {
  private final Tracer tracer;

  public ProteusTracerSupplier(Proteus proteus, Optional<String> tracingGroup) {
    ProteusSocket proteusSocket = proteus.group(tracingGroup.orElse("com.netifi.proteus.tracing"));

    ProteusTracingServiceClient client = new ProteusTracingServiceClient(proteusSocket);
    ProteusReporter reporter = new ProteusReporter(client, proteus.getGroupName(), proteus.getDestination());

    Tracing tracing =
        Tracing.newBuilder()
            .spanReporter(reporter)
            .build();

    tracer = BraveTracer.create(tracing);
  }

  @Override
  public Tracer get() {
    return tracer;
  }
}
