package io.netifi.testing.protobuf;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.4.0)",
    comments = "Source: io.netifi.sdk.proteus/simpleservice.proto")
public final class SimpleServiceClient implements SimpleService {
  private final io.rsocket.RSocket rSocket;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<Void>, ? extends org.reactivestreams.Publisher<Void>> fireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<SimpleResponse>, ? extends org.reactivestreams.Publisher<SimpleResponse>> streamOnFireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<SimpleResponse>, ? extends org.reactivestreams.Publisher<SimpleResponse>> unaryRpc;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<SimpleResponse>, ? extends org.reactivestreams.Publisher<SimpleResponse>> clientStreamingRpc;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<SimpleResponse>, ? extends org.reactivestreams.Publisher<SimpleResponse>> serverStreamingRpc;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<SimpleResponse>, ? extends org.reactivestreams.Publisher<SimpleResponse>> serverStreamingFireHose;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<SimpleResponse>, ? extends org.reactivestreams.Publisher<SimpleResponse>> bidiStreamingRpc;

  public SimpleServiceClient(io.rsocket.RSocket rSocket) {
    this.rSocket = rSocket;
    this.fireAndForget = java.util.function.Function.identity();
    this.streamOnFireAndForget = java.util.function.Function.identity();
    this.unaryRpc = java.util.function.Function.identity();
    this.clientStreamingRpc = java.util.function.Function.identity();
    this.serverStreamingRpc = java.util.function.Function.identity();
    this.serverStreamingFireHose = java.util.function.Function.identity();
    this.bidiStreamingRpc = java.util.function.Function.identity();
  }

  public SimpleServiceClient(io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry) {
    this.rSocket = rSocket;
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "fireAndForget");
    this.streamOnFireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "streamOnFireAndForget");
    this.unaryRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "unaryRpc");
    this.clientStreamingRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "clientStreamingRpc");
    this.serverStreamingRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "serverStreamingRpc");
    this.serverStreamingFireHose = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "serverStreamingFireHose");
    this.bidiStreamingRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "bidiStreamingRpc");
  }

  public reactor.core.publisher.Mono<Void> fireAndForget(SimpleRequest message) {
    return fireAndForget(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Mono<Void> fireAndForget(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Mono.defer(new java.util.function.Supplier<reactor.core.publisher.Mono<Void>>() {
      @Override
      public reactor.core.publisher.Mono<Void> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_FIRE_AND_FORGET, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.fireAndForget(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).transform(fireAndForget);
  }

  public reactor.core.publisher.Flux<SimpleResponse> streamOnFireAndForget(Empty message) {
    return streamOnFireAndForget(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Flux<SimpleResponse> streamOnFireAndForget(Empty message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Flux.defer(new java.util.function.Supplier<reactor.core.publisher.Flux<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Flux<io.rsocket.Payload> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_STREAM_ON_FIRE_AND_FORGET, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestStream(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(SimpleResponse.parser())).transform(streamOnFireAndForget);
  }

  public reactor.core.publisher.Mono<SimpleResponse> unaryRpc(SimpleRequest message) {
    return unaryRpc(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Mono<SimpleResponse> unaryRpc(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Mono.defer(new java.util.function.Supplier<reactor.core.publisher.Mono<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Mono<io.rsocket.Payload> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_UNARY_RPC, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestResponse(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(SimpleResponse.parser())).transform(unaryRpc);
  }

  public reactor.core.publisher.Mono<SimpleResponse> clientStreamingRpc(org.reactivestreams.Publisher<SimpleRequest> messages) {
    return clientStreamingRpc(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Mono<SimpleResponse> clientStreamingRpc(org.reactivestreams.Publisher<SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    return rSocket.requestChannel(reactor.core.publisher.Flux.from(messages).map(
      new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        boolean once;

        @Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (!once) {
            final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
            final io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
            io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_CLIENT_STREAMING_RPC, metadata);
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(SimpleResponse.parser())).single().transform(clientStreamingRpc);
  }

  public reactor.core.publisher.Flux<SimpleResponse> serverStreamingRpc(SimpleRequest message) {
    return serverStreamingRpc(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Flux<SimpleResponse> serverStreamingRpc(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Flux.defer(new java.util.function.Supplier<reactor.core.publisher.Flux<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Flux<io.rsocket.Payload> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_SERVER_STREAMING_RPC, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestStream(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(SimpleResponse.parser())).transform(serverStreamingRpc);
  }

  public reactor.core.publisher.Flux<SimpleResponse> serverStreamingFireHose(SimpleRequest message) {
    return serverStreamingFireHose(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Flux<SimpleResponse> serverStreamingFireHose(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Flux.defer(new java.util.function.Supplier<reactor.core.publisher.Flux<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Flux<io.rsocket.Payload> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_SERVER_STREAMING_FIRE_HOSE, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestStream(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(SimpleResponse.parser())).transform(serverStreamingFireHose);
  }

  public reactor.core.publisher.Flux<SimpleResponse> bidiStreamingRpc(org.reactivestreams.Publisher<SimpleRequest> messages) {
    return bidiStreamingRpc(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public reactor.core.publisher.Flux<SimpleResponse> bidiStreamingRpc(org.reactivestreams.Publisher<SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    return rSocket.requestChannel(reactor.core.publisher.Flux.from(messages).map(
      new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        boolean once;

        @Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (!once) {
            final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
            final io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
            io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_BIDI_STREAMING_RPC, metadata);
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(SimpleResponse.parser())).transform(bidiStreamingRpc);
  }

  private static io.netty.buffer.ByteBuf serialize(final com.google.protobuf.MessageLite message) {
    io.netty.buffer.ByteBuf byteBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(message.getSerializedSize());
    try {
      message.writeTo(com.google.protobuf.CodedOutputStream.newInstance(byteBuf.nioBuffer(0, byteBuf.writableBytes())));
      byteBuf.writerIndex(byteBuf.capacity());
      return byteBuf;
    } catch (Throwable t) {
      byteBuf.release();
      throw new RuntimeException(t);
    }
  }

  private static <T> java.util.function.Function<io.rsocket.Payload, T> deserializer(final com.google.protobuf.Parser<T> parser) {
    return new java.util.function.Function<io.rsocket.Payload, T>() {
      @Override
      public T apply(io.rsocket.Payload payload) {
        try {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return parser.parseFrom(is);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        } finally {
          payload.release();
        }
      }
    };
  }
}
