package io.netifi.proteus;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import reactor.core.publisher.Flux;

public interface ProteusService extends RSocket {
  int getNamespaceId();

  int getServiceId();

  Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher);
}
