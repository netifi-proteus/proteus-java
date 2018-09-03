package io.netifi.proteus.tracing;

import io.netifi.proteus.Proteus;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Function;

public class ProteusTraceStreamsSupplier implements Function<TracesRequest, Flux<Trace>> {

  private final ProteusTracingServiceClient client;

  @Inject
  public ProteusTraceStreamsSupplier(Proteus proteus, Optional<String> tracingGroup) {
    client = new ProteusTracingServiceClient(proteus.group(tracingGroup.orElse("com.netifi.proteus.tracing")));
  }

  @Override
  public Flux<Trace> apply(TracesRequest message) {
    return client.streamTraces(message);
  }
}
