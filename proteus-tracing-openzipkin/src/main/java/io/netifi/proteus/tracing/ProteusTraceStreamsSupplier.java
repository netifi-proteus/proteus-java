package io.netifi.proteus.tracing;

import io.netifi.proteus.Proteus;
import io.rsocket.RSocket;
import java.util.Optional;
import java.util.function.Function;
import javax.inject.Inject;
import reactor.core.publisher.Flux;

public class ProteusTraceStreamsSupplier implements Function<TracesRequest, Flux<Trace>> {

  private final ProteusTracingServiceClient client;

  public ProteusTraceStreamsSupplier(Proteus proteus, Optional<String> tracingGroup) {
    this(proteus::group, tracingGroup);
  }

  @Inject
  public ProteusTraceStreamsSupplier(
      Function<String, RSocket> rSocketFactory, Optional<String> tracingGroup) {
    client =
        new ProteusTracingServiceClient(
            rSocketFactory.apply(tracingGroup.orElse("com.netifi.proteus.tracing")));
  }

  @Override
  public Flux<Trace> apply(TracesRequest message) {
    return client.streamTraces(message);
  }
}
