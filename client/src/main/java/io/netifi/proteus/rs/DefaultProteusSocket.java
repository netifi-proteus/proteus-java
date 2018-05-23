package io.netifi.proteus.rs;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.function.Function;
import java.util.function.Supplier;

public class DefaultProteusSocket implements ProteusSocket {
  private final Function<Payload, Payload> payloadTransformer;
  private final Supplier<RSocket> rSocketSupplier;
  private final MonoProcessor<Void> onClose;

  public DefaultProteusSocket(
      Function<Payload, Payload> payloadTransformer, Supplier<RSocket> rSocketSupplier) {
    this.payloadTransformer = payloadTransformer;
    this.rSocketSupplier = rSocketSupplier;
    this.onClose = MonoProcessor.create();
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return Mono.defer(
        () -> {
          Payload transformedPayload = payloadTransformer.apply(payload);
          if (transformedPayload != null) {
            ReferenceCountUtil.safeRelease(payload);
          }

          return rSocketSupplier.get().fireAndForget(transformedPayload);
        });
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return Mono.defer(
        () -> {
          Payload transformedPayload = payloadTransformer.apply(payload);
          if (transformedPayload != null) {
            ReferenceCountUtil.safeRelease(payload);
          }

          return rSocketSupplier.get().requestResponse(transformedPayload);
        });
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return Flux.defer(
        () -> {
          Payload transformedPayload = payloadTransformer.apply(payload);
          if (transformedPayload != null) {
            ReferenceCountUtil.safeRelease(payload);
          }

          return rSocketSupplier.get().requestStream(transformedPayload);
        });
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    Flux<Payload> transformed =
        Flux.from(payloads)
            .map(
                payload -> {
                  Payload transformedPayload = payloadTransformer.apply(payload);
                  if (transformedPayload != null) {
                    ReferenceCountUtil.safeRelease(payload);
                  }
                  return transformedPayload;
                });

    return rSocketSupplier.get().requestChannel(transformed);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return Mono.defer(
        () -> {
          Payload transformedPayload = payloadTransformer.apply(payload);
          if (transformedPayload != null) {
            ReferenceCountUtil.safeRelease(payload);
          }

          return rSocketSupplier.get().metadataPush(transformedPayload);
        });
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
}
