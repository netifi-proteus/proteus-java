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
