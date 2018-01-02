package io.netifi.testing.protobuf;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.4.0)",
    comments = "Source: io.netifi.sdk.proteus/simpleservice.proto")
public final class SimpleServiceServer extends io.netifi.proteus.AbstractProteusService {
  private final SimpleService service;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<Void>, ? extends org.reactivestreams.Publisher<Void>> fireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> streamOnFireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> unaryRpc;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> clientStreamingRpc;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> serverStreamingRpc;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> serverStreamingFireHose;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> bidiStreamingRpc;

  public SimpleServiceServer(SimpleService service) {
    this.service = service;
    this.fireAndForget = java.util.function.Function.identity();
    this.streamOnFireAndForget = java.util.function.Function.identity();
    this.unaryRpc = java.util.function.Function.identity();
    this.clientStreamingRpc = java.util.function.Function.identity();
    this.serverStreamingRpc = java.util.function.Function.identity();
    this.serverStreamingFireHose = java.util.function.Function.identity();
    this.bidiStreamingRpc = java.util.function.Function.identity();
  }

  public SimpleServiceServer(SimpleService service, io.micrometer.core.instrument.MeterRegistry registry) {
    this.service = service;
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "fireAndForget");
    this.streamOnFireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "streamOnFireAndForget");
    this.unaryRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "unaryRpc");
    this.clientStreamingRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "clientStreamingRpc");
    this.serverStreamingRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "serverStreamingRpc");
    this.serverStreamingFireHose = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "serverStreamingFireHose");
    this.bidiStreamingRpc = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.testing", "service", "SimpleService", "method", "bidiStreamingRpc");
  }

  @Override
  public int getNamespaceId() {
    return SimpleService.NAMESPACE_ID;
  }

  @Override
  public int getServiceId() {
    return SimpleService.SERVICE_ID;
  }

  @Override
  public reactor.core.publisher.Mono<Void> fireAndForget(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_FIRE_AND_FORGET: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.fireAndForget(SimpleRequest.parseFrom(is), metadata);
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

  @Override
  public reactor.core.publisher.Mono<io.rsocket.Payload> requestResponse(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_UNARY_RPC: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.unaryRpc(SimpleRequest.parseFrom(is), metadata).map(serializer).transform(unaryRpc);
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

  @Override
  public reactor.core.publisher.Flux<io.rsocket.Payload> requestStream(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_STREAM_ON_FIRE_AND_FORGET: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.streamOnFireAndForget(Empty.parseFrom(is), metadata).map(serializer).transform(streamOnFireAndForget);
        }
        case SimpleService.METHOD_SERVER_STREAMING_RPC: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.serverStreamingRpc(SimpleRequest.parseFrom(is), metadata).map(serializer).transform(serverStreamingRpc);
        }
        case SimpleService.METHOD_SERVER_STREAMING_FIRE_HOSE: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.serverStreamingFireHose(SimpleRequest.parseFrom(is), metadata).map(serializer).transform(serverStreamingFireHose);
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

  @Override
  public reactor.core.publisher.Flux<io.rsocket.Payload> requestChannel(io.rsocket.Payload payload, reactor.core.publisher.Flux<io.rsocket.Payload> publisher) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_CLIENT_STREAMING_RPC: {
          reactor.core.publisher.Flux<SimpleRequest> messages =
            publisher.map(deserializer(SimpleRequest.parser()));
          return service.clientStreamingRpc(messages, metadata).map(serializer).transform(clientStreamingRpc).flux();
        }
        case SimpleService.METHOD_BIDI_STREAMING_RPC: {
          reactor.core.publisher.Flux<SimpleRequest> messages =
            publisher.map(deserializer(SimpleRequest.parser()));
          return service.bidiStreamingRpc(messages, metadata).map(serializer).transform(bidiStreamingRpc);
        }
        default: {
          return reactor.core.publisher.Flux.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return reactor.core.publisher.Flux.error(t);
    }
  }

  @Override
  public reactor.core.publisher.Flux<io.rsocket.Payload> requestChannel(org.reactivestreams.Publisher<io.rsocket.Payload> payloads) {
    return new io.rsocket.internal.SwitchTransform<io.rsocket.Payload, io.rsocket.Payload>(payloads, new java.util.function.BiFunction<io.rsocket.Payload, reactor.core.publisher.Flux<io.rsocket.Payload>, org.reactivestreams.Publisher<? extends io.rsocket.Payload>>() {
      @Override
      public org.reactivestreams.Publisher<io.rsocket.Payload> apply(io.rsocket.Payload payload, reactor.core.publisher.Flux<io.rsocket.Payload> publisher) {
        return requestChannel(payload, publisher);
      }
    });
  }

  private static final java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload> serializer =
    new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
      @Override
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
