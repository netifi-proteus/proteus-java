package io.netifi.proteus.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Lets you wrap socket with */
abstract class AbstractUnwrappingRSocket extends RSocketProxy {

  AbstractUnwrappingRSocket(RSocket source) {
    super(source);
  }

  protected abstract Payload unwrap(Payload payload);

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      return super.fireAndForget(unwrap(payload));
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      return super.requestResponse(unwrap(payload));
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      return super.requestStream(unwrap(payload));
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return super.requestChannel(Flux.from(payloads).map(this::unwrap));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return super.metadataPush(unwrap(payload));
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }
}
