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

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.ResponderRSocket;
import io.rsocket.rpc.RSocketRpcService;
import io.rsocket.rpc.frames.Metadata;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class NamedRSocketServiceWrapper extends AbstractUnwrappingRSocket
    implements RSocketRpcService {
  private final String name;

  private NamedRSocketServiceWrapper(String name, RSocket source) {
    super(source);
    this.name = name;
  }

  /**
   * Wraps an RSocket with {@link RSocketProxy} and {@link RSocketRpcService}
   *
   * @param name what you want your RSocket to be found as
   * @param source the raw socket to handle to wrap
   * @return a new NamedRSocketServiceWrapper instance
   */
  public static NamedRSocketServiceWrapper wrap(String name, RSocket source) {
    return new NamedRSocketServiceWrapper(name, source);
  }

  @Override
  protected Payload unwrap(Payload payload) {
    try {
      ByteBuf data = payload.sliceData();
      ByteBuf metadata = payload.sliceMetadata();
      ByteBuf unwrappedMetadata = Metadata.getMetadata(metadata);

      return ByteBufPayload.create(data.retain(), unwrappedMetadata.retain());
    } finally {
      payload.release();
    }
  }

  @Override
  public String getService() {
    return name;
  }

  @Override
  public final Flux<Payload> requestChannel(Payload payload, Publisher<Payload> publisher) {
    return requestChannel(payload, Flux.from(publisher));
  }

  @Override
  public final Flux<Payload> requestChannel(Payload payload, Flux<Payload> payloads) {
    if (source instanceof ResponderRSocket) {
      ResponderRSocket responderRSocket = (ResponderRSocket) source;

      return responderRSocket.requestChannel(unwrap(payload), payloads.map(this::unwrap));
    }

    return super.requestChannel(payloads);
  }
}
