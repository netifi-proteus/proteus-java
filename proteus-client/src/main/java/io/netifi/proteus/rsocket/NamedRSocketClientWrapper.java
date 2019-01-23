package io.netifi.proteus.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.rpc.frames.Metadata;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class NamedRSocketClientWrapper extends RSocketProxy implements ProteusSocket {
  private final String name;

  private NamedRSocketClientWrapper(String name, RSocket source) {
    super(source);
    this.name = name;
  }

  /**
   * Wraps an RSocket with {@link RSocketProxy} and RSocketRpcService
   *
   * @param name what you want your RSocket to be found as
   * @param source the raw socket to handle to wrap
   * @return a new NamedRSocketClientWrapper instance
   */
  public static NamedRSocketClientWrapper wrap(String name, RSocket source) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(source);
    return new NamedRSocketClientWrapper(name, source);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return source.fireAndForget(wrap(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return source.requestResponse(wrap(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return source.requestStream(wrap(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return source.requestChannel(Flux.from(payloads).map(this::wrap));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return source.metadataPush(wrap(payload));
  }

  private Payload wrap(Payload payload) {
    ByteBuf metadata =
        Metadata.encode(ByteBufAllocator.DEFAULT, name, name, payload.sliceMetadata());

    return ByteBufPayload.create(payload.sliceData().retain(), metadata);
  }
}
