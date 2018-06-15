package io.netifi.proteus.rpc.blocking;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.7.1-SNAPSHOT)",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
public final class SimpleServiceClient2 implements SimpleService {
  private final io.rsocket.RSocket rSocket;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> requestReply;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<Void>, ? extends org.reactivestreams.Publisher<Void>> fireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> requestStream;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> streamingRequestSingleResponse;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>, ? extends org.reactivestreams.Publisher<io.netifi.proteus.rpc.SimpleResponse>> streamingRequestAndResponse;
  private java.time.Duration deafultTimeout = java.time.Duration.ofSeconds(30);
  
  
  public SimpleServiceClient2(io.rsocket.RSocket rSocket) {
    this.rSocket = rSocket;
    this.requestReply = java.util.function.Function.identity();
    this.fireAndForget = java.util.function.Function.identity();
    this.requestStream = java.util.function.Function.identity();
    this.streamingRequestSingleResponse = java.util.function.Function.identity();
    this.streamingRequestAndResponse = java.util.function.Function.identity();
  }
  
  public SimpleServiceClient2(io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry) {
    this.rSocket = rSocket;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestReply");
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "fireAndForget");
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestStream");
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestSingleResponse");
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestAndResponse");
  }
  
  public SimpleServiceClient2(io.rsocket.RSocket rSocket, java.time.Duration deafultTimeout) {
    this.rSocket = rSocket;
    this.requestReply = java.util.function.Function.identity();
    this.fireAndForget = java.util.function.Function.identity();
    this.requestStream = java.util.function.Function.identity();
    this.streamingRequestSingleResponse = java.util.function.Function.identity();
    this.streamingRequestAndResponse = java.util.function.Function.identity();
    this.deafultTimeout = deafultTimeout;
  }
  
  public SimpleServiceClient2(io.rsocket.RSocket rSocket, java.time.Duration deafultTimeout, io.micrometer.core.instrument.MeterRegistry registry) {
    this.rSocket = rSocket;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestReply");
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "fireAndForget");
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestStream");
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestSingleResponse");
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestAndResponse");
    this.deafultTimeout = deafultTimeout;
  }

  public io.netifi.proteus.rpc.SimpleResponse requestReply(io.netifi.proteus.rpc.SimpleRequest message) {
    return requestReply(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.rpc.SimpleResponse requestReply(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return reactor.core.publisher.Mono.defer(new java.util.function.Supplier<reactor.core.publisher.Mono<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Mono<io.rsocket.Payload> get() {
        final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_REQUEST_REPLY, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestResponse(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).transform(requestReply)
      .block();
  }

  public void fireAndForget(io.netifi.proteus.rpc.SimpleRequest message) {
     fireAndForget(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public void fireAndForget(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
     reactor.core.publisher.Mono.defer(new java.util.function.Supplier<reactor.core.publisher.Mono<Void>>() {
      @Override
      public reactor.core.publisher.Mono<Void> get() {
        final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_FIRE_AND_FORGET, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.fireAndForget(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).transform(fireAndForget)
      .block();
  }

  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message) {
    return requestStream(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleResponse> stream = reactor.core.publisher.Flux.defer(new java.util.function.Supplier<reactor.core.publisher.Flux<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Flux<io.rsocket.Payload> get() {
        final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_REQUEST_STREAM, metadata);
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestStream(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).transform(requestStream);
    
    return new io.netifi.proteus.BlockingIterable<>(stream, reactor.util.concurrent.Queues.SMALL_BUFFER_SIZE, reactor.util.concurrent.Queues.small());
  }

  public io.netifi.proteus.rpc.SimpleResponse streamingRequestSingleResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages) {
    return streamingRequestSingleResponse(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.rpc.SimpleResponse streamingRequestSingleResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    return rSocket.requestChannel(reactor.core.publisher.Flux.fromIterable(messages).map(
      new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        private final java.util.concurrent.atomic.AtomicBoolean once = new java.util.concurrent.atomic.AtomicBoolean(false);

        @Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (once.compareAndSet(false, true)) {
            final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE, metadata);
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).single().transform(streamingRequestSingleResponse)
        .block();
  }

  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages) {
    return streamingRequestAndResponse(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleResponse> stream = rSocket.requestChannel(reactor.core.publisher.Flux.fromIterable(messages).map(
        new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
          private final java.util.concurrent.atomic.AtomicBoolean once = new java.util.concurrent.atomic.AtomicBoolean(false);
        
          @Override
          public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
            io.netty.buffer.ByteBuf data = serialize(message);
            if (once.compareAndSet(false, true)) {
              final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.NAMESPACE_ID, SimpleService.SERVICE_ID, SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE, metadata);
              return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
            } else {
              return io.rsocket.util.ByteBufPayload.create(data);
            }
          }
        })).map(deserializer(io.netifi.proteus.rpc.SimpleResponse.parser())).transform(streamingRequestAndResponse);
  
    return new io.netifi.proteus.BlockingIterable<>(stream, reactor.util.concurrent.Queues.SMALL_BUFFER_SIZE, reactor.util.concurrent.Queues.small());
  }

  private static io.netty.buffer.ByteBuf serialize(final com.google.protobuf.MessageLite message) {
    int length = message.getSerializedSize();
    io.netty.buffer.ByteBuf byteBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.buffer(length);
    try {
      message.writeTo(com.google.protobuf.CodedOutputStream.newInstance(byteBuf.internalNioBuffer(0, length)));
      byteBuf.writerIndex(length);
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
