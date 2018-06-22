package io.netifi.proteus;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import reactor.core.publisher.Flux;

public abstract class AbstractProteusService extends AbstractRSocket implements ProteusService {
  @Override
  public String getService() {
    return getClass().getName();
  }

  @Override
  public Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher) {
    return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
  }

  @Override
  public double availability() {
    return 1.0;
  }
  
  public abstract Class<?> getServiceClass();
}
