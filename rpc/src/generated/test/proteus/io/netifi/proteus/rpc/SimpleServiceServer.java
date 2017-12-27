package io.netifi.proteus.rpc;

@javax.annotation.Generated(
    value = "by Proteus proto compiler",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
public final class SimpleServiceServer extends io.netifi.proteus.AbstractProteusService {
  private final SimpleService service;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> requestReply;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<Void>, ? extends org.reactivestreams.Publisher<Void>> fireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> requestStream;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> streamingRequestSingleResponse;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> streamingRequestAndResponse;

  public SimpleServiceServer(SimpleService service) {
    this.service = service;
    this.requestReply = java.util.function.Function.identity();
    this.fireAndForget = java.util.function.Function.identity();
    this.requestStream = java.util.function.Function.identity();
    this.streamingRequestSingleResponse = java.util.function.Function.identity();
    this.streamingRequestAndResponse = java.util.function.Function.identity();
  }

  public SimpleServiceServer(SimpleService service, io.micrometer.core.instrument.MeterRegistry registry) {
    this.service = service;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestReply");
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "fireAndForget");
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestStream");
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestSingleResponse");
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestAndResponse");
  }

  @java.lang.Override
  public int getNamespaceId() {
    return SimpleService.NAMESPACE_ID;
  }

  @java.lang.Override
  public int getServiceId() {
    return SimpleService.SERVICE_ID;
  }

  @java.lang.Override
  public reactor.core.publisher.Mono<Void> fireAndForget(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_FIRE_AND_FORGET: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.fireAndForget(io.netifi.proteus.rpc.SimpleRequest.parseFrom(is), metadata);
        }
        default: {
          return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return reactor.core.publisher.Mono.error(t);
    } finally {
      payload.release();
    }
  }

  @java.lang.Override
  public reactor.core.publisher.Mono<io.rsocket.Payload> requestResponse(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_REQUEST_REPLY: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.requestReply(io.netifi.proteus.rpc.SimpleRequest.parseFrom(is), metadata).map(serializer).transform(requestReply);
        }
        default: {
          return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return reactor.core.publisher.Mono.error(t);
    } finally {
      payload.release();
    }
  }

  @java.lang.Override
  public reactor.core.publisher.Flux<io.rsocket.Payload> requestStream(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_REQUEST_STREAM: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.requestStream(io.netifi.proteus.rpc.SimpleRequest.parseFrom(is), metadata).map(serializer).transform(requestStream);
        }
        default: {
          return reactor.core.publisher.Flux.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return reactor.core.publisher.Flux.error(t);
    } finally {
      payload.release();
    }
  }

  @java.lang.Override
  public reactor.core.publisher.Flux<io.rsocket.Payload> requestChannel(io.rsocket.Payload payload, reactor.core.publisher.Flux<io.rsocket.Payload> publisher) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE: {
          reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleRequest> messages =
            publisher.map(deserializer(io.netifi.proteus.rpc.SimpleRequest.parser()));
          return service.streamingRequestSingleResponse(messages, metadata).map(serializer).transform(streamingRequestSingleResponse).flux();
        }
        case SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE: {
          reactor.core.publisher.Flux<io.netifi.proteus.rpc.SimpleRequest> messages =
            publisher.map(deserializer(io.netifi.proteus.rpc.SimpleRequest.parser()));
          return service.streamingRequestAndResponse(messages, metadata).map(serializer).transform(streamingRequestAndResponse);
        }
        default: {
          return reactor.core.publisher.Flux.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return reactor.core.publisher.Flux.error(t);
    }
  }

  @java.lang.Override
  public reactor.core.publisher.Flux<io.rsocket.Payload> requestChannel(org.reactivestreams.Publisher<io.rsocket.Payload> payloads) {
    return new io.rsocket.internal.SwitchTransform<io.rsocket.Payload, io.rsocket.Payload>(payloads, new java.util.function.BiFunction<io.rsocket.Payload, reactor.core.publisher.Flux<io.rsocket.Payload>, org.reactivestreams.Publisher<? extends io.rsocket.Payload>>() {
      @java.lang.Override
      public org.reactivestreams.Publisher<io.rsocket.Payload> apply(io.rsocket.Payload payload, reactor.core.publisher.Flux<io.rsocket.Payload> publisher) {
        return requestChannel(payload, publisher);
      }
    });
  }

  private static final java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload> serializer =
    new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
      @java.lang.Override
      public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
        io.netty.buffer.ByteBuf byteBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.directBuffer(message.getSerializedSize());
        try {
          message.writeTo(com.google.protobuf.CodedOutputStream.newInstance(byteBuf.nioBuffer(0, byteBuf.writableBytes())));
          byteBuf.writerIndex(byteBuf.capacity());
          return io.rsocket.util.ByteBufPayload.create(byteBuf);
        } catch (Throwable t) {
          byteBuf.release();
          throw new RuntimeException(t);
        }
      }
    };

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
