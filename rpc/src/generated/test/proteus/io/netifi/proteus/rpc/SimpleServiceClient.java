package io.netifi.proteus.rpc;

@javax.annotation.Generated(
    value = "by Proteus proto compiler",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
public final class SimpleServiceClient implements SimpleService {
  private final io.rsocket.RSocket rSocket;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> requestReply;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<Void>, ? extends org.reactivestreams.Publisher<Void>> fireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> requestStream;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> streamingRequestSingleResponse;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> streamingRequestAndResponse;

  public SimpleServiceClient(io.rsocket.RSocket rSocket) {
    this.rSocket = rSocket;
    this.requestReply = java.util.function.Function.identity();
    this.fireAndForget = java.util.function.Function.identity();
    this.requestStream = java.util.function.Function.identity();
    this.streamingRequestSingleResponse = java.util.function.Function.identity();
    this.streamingRequestAndResponse = java.util.function.Function.identity();
  }

  public SimpleServiceClient(io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry) {
    this.rSocket = rSocket;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestReply");
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "fireAndForget");
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestStream");
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestSingleResponse");
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestAndResponse");
  }

  public reactor.core.publisher.Mono<io.netifi.proteus.rpc.SimpleResponse> requestReply(io.netifi.proteus.rpc.SimpleRequest message) {
    return requestReply(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @java.lang.Override
  public reactor.core.publisher.Mono<io.netifi.proteus.rpc.SimpleResponse> requestReply(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Mono.defer(new java.util.function.Supplier<reactor.core.publisher.Mono<io.rsocket.Payload>>() {
      @java.lang.Override
      public reactor.core.publisher.Mono<io.rsocket.Payload> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_REQUEST_REPLY, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestResponse(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).transform(requestReply);
  }

  public reactor.core.publisher.Mono<Void> fireAndForget(io.netifi.proteus.rpc.SimpleRequest message) {
    return fireAndForget(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @java.lang.Override
  public reactor.core.publisher.Mono<Void> fireAndForget(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Mono.defer(new java.util.function.Supplier<reactor.core.publisher.Mono<Void>>() {
      @java.lang.Override
      public reactor.core.publisher.Mono<Void> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_FIRE_AND_FORGET, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.fireAndForget(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).transform(fireAndForget);
  }

  public reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message) {
    return requestStream(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @java.lang.Override
  public reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Flux.defer(new java.util.function.Supplier<reactor.core.publisher.Flux<io.rsocket.Payload>>() {
      @java.lang.Override
      public reactor.core.publisher.Flux<io.rsocket.Payload> get() {
        final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
        io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
        io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_REQUEST_STREAM, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestStream(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).transform(requestStream);
  }

  public reactor.core.publisher.Mono<io.netifi.proteus.rpc.SimpleResponse> streamingRequestSingleResponse(org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleRequest> messages) {
    return streamingRequestSingleResponse(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @java.lang.Override
  public reactor.core.publisher.Mono<io.netifi.proteus.rpc.SimpleResponse> streamingRequestSingleResponse(org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    return rSocket.requestChannel(reactor.core.publisher.Flux.from(messages).map(
      new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        boolean once;

        @java.lang.Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (!once) {
            final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
            final io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
            io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE, metadata);
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).single().transform(streamingRequestSingleResponse);
  }

  public reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleRequest> messages) {
    return streamingRequestAndResponse(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @java.lang.Override
  public reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    return rSocket.requestChannel(reactor.core.publisher.Flux.from(messages).map(
      new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        boolean once;

        @java.lang.Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (!once) {
            final int length = io.netifi.proteus.frames.ProteusMetadata.computeLength();
            final io.netty.buffer.ByteBuf metadataBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(length);
            io.netifi.proteus.frames.ProteusMetadata.encode(metadataBuf, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE, metadata);
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).transform(streamingRequestAndResponse);
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
      @java.lang.Override
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
