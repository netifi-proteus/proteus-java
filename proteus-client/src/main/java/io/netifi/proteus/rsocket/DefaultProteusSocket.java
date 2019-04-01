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
package io.netifi.proteus.rsocket;

import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultProteusSocket implements ProteusSocket {

  private static final Logger logger = LoggerFactory.getLogger(DefaultProteusSocket.class);
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
          if (transformedPayload != null && payload.refCnt() > 0) {
            quietRelease(payload);
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
            quietRelease(payload);
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
            quietRelease(payload);
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
                    quietRelease(payload);
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
            quietRelease(payload);
          }

          return rSocketSupplier.get().metadataPush(transformedPayload);
        });
  }

  private static void quietRelease(ReferenceCounted ref) {
    try {
      if (ref.refCnt() > 0) {
        ref.release();
      }
    } catch (Throwable t) {
      logger.trace("error releasing", t);
    }
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
