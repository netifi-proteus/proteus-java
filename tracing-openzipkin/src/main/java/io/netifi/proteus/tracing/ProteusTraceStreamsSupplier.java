package io.netifi.proteus.tracing;

import com.google.protobuf.Empty;
import io.netifi.proteus.Proteus;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Supplier;

public class ProteusTraceStreamsSupplier implements Supplier<Flux<Trace>> {

  private final ProteusTracingServiceClient client;

  @Inject
  public ProteusTraceStreamsSupplier(Proteus proteus, Optional<String> tracingGroup) {
    client = new ProteusTracingServiceClient(proteus.group(tracingGroup.orElse("com.netifi.proteus.tracing")));
  }

  @Override
  public Flux<Trace> get() {
    return client.streamTraces(Empty.getDefaultInstance());
  }
}
