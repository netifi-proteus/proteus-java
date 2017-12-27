package io.netifi.proteus;

import io.netifi.proteus.util.Hashing;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import reactor.core.publisher.Flux;

public abstract class AbstractProteusService extends AbstractRSocket implements ProteusService {
  @Override
  public int getNamespaceId() {
    return Hashing.hash(getClass().getPackage().getName());
  }

  @Override
  public int getServiceId() {
    return Hashing.hash(getClass().getName());
  }

  @Override
  public Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher) {
    return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
  }

  @Override
  public double availability() {
    return 1.0;
  }
}
