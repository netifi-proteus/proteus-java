package io.netifi.proteus.rpc.blocking;


import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.7.1-SNAPSHOT)",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
public final class SimpleServiceServer extends io.netifi.proteus.AbstractProteusService {
  private final SimpleService service;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> requestReply;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<Void>, ? extends org.reactivestreams.Publisher<Void>> fireAndForget;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> requestStream;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> streamingRequestSingleResponse;
  private final java.util.function.Function<? super org.reactivestreams.Publisher<io.rsocket.Payload>, ? extends org.reactivestreams.Publisher<io.rsocket.Payload>> streamingRequestAndResponse;
  private final reactor.core.scheduler.Scheduler scheduler;
  
  
  public SimpleServiceServer(SimpleService service) {
    this.service = service;
    this.scheduler = reactor.core.scheduler.Schedulers.elastic();
    this.requestReply = java.util.function.Function.identity();
    this.fireAndForget = java.util.function.Function.identity();
    this.requestStream = java.util.function.Function.identity();
    this.streamingRequestSingleResponse = java.util.function.Function.identity();
    this.streamingRequestAndResponse = java.util.function.Function.identity();
  }
  
  public SimpleServiceServer(SimpleService service, io.micrometer.core.instrument.MeterRegistry registry) {
    this.service = service;
    this.scheduler = reactor.core.scheduler.Schedulers.elastic();
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestReply");
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "fireAndForget");
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestStream");
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestSingleResponse");
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestAndResponse");
  }
  
  
  public SimpleServiceServer(SimpleService service, reactor.core.scheduler.Scheduler scheduler) {
    this.service = service;
    this.scheduler = scheduler;
    this.requestReply = java.util.function.Function.identity();
    this.fireAndForget = java.util.function.Function.identity();
    this.requestStream = java.util.function.Function.identity();
    this.streamingRequestSingleResponse = java.util.function.Function.identity();
    this.streamingRequestAndResponse = java.util.function.Function.identity();
  }
  
  public SimpleServiceServer(SimpleService service, reactor.core.scheduler.Scheduler scheduler, io.micrometer.core.instrument.MeterRegistry registry) {
    this.service = service;
    this.scheduler = scheduler;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestReply");
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "fireAndForget");
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "requestStream");
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestSingleResponse");
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.server", "namespace", "io.netifi.proteus.rpc", "service", "SimpleService", "method", "streamingRequestAndResponse");
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
  public Mono<Void> fireAndForget(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_FIRE_AND_FORGET: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          io.netifi.proteus.rpc.SimpleRequest simpleRequest = io.netifi.proteus.rpc.SimpleRequest.parseFrom(is);
          return Mono.<Void>fromRunnable(() -> service.fireAndForget(simpleRequest, metadata))
          .subscribeOn(scheduler);
        }
        default: {
          return Mono.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return Mono.error(t);
    } finally {
      payload.release();
    }
  }
  @Override
  public Mono<io.rsocket.Payload> requestResponse(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_REQUEST_REPLY: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          io.netifi.proteus.rpc.SimpleRequest simpleRequest = io.netifi.proteus.rpc.SimpleRequest.parseFrom(is);
          return Mono.fromSupplier(() -> service.requestReply(simpleRequest, metadata)).map(serializer).transform(requestReply).subscribeOn(scheduler);
        }
        default: {
          return Mono.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return Mono.error(t);
    } finally {
      payload.release();
    }
  }

  @Override
  public Flux<io.rsocket.Payload> requestStream(io.rsocket.Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_REQUEST_STREAM: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          io.netifi.proteus.rpc.SimpleRequest simpleRequest = io.netifi.proteus.rpc.SimpleRequest.parseFrom(is);
          return Flux.fromIterable(service.requestStream(simpleRequest, metadata)).map(serializer).transform(requestStream).subscribeOn(scheduler);
        }
        default: {
          return Flux.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return Flux.error(t);
    } finally {
      payload.release();
    }
  }

  @Override
  public Flux<io.rsocket.Payload> requestChannel(io.rsocket.Payload payload, Flux<io.rsocket.Payload> publisher) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      switch(io.netifi.proteus.frames.ProteusMetadata.methodId(metadata)) {
        case SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE: {
          Flux<io.netifi.proteus.rpc.SimpleRequest> messages =
            publisher.map(deserializer(io.netifi.proteus.rpc.SimpleRequest.parser()));
          
          return Mono.fromSupplier(() -> service.streamingRequestSingleResponse(messages.toIterable(), metadata)).map(serializer).transform(streamingRequestSingleResponse).flux().subscribeOn(scheduler);
        }
        case SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE: {
          Flux<io.netifi.proteus.rpc.SimpleRequest> messages =
            publisher.map(deserializer(io.netifi.proteus.rpc.SimpleRequest.parser()));
          return Flux.defer(() -> Flux.fromIterable(service.streamingRequestAndResponse(messages.toIterable(), metadata))).map(serializer).transform(streamingRequestAndResponse).subscribeOn(scheduler);
        }
        default: {
          return Flux.error(new UnsupportedOperationException());
        }
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<io.rsocket.Payload> requestChannel(org.reactivestreams.Publisher<io.rsocket.Payload> payloads) {
    return new io.rsocket.internal.SwitchTransform<io.rsocket.Payload, io.rsocket.Payload>(payloads, new java.util.function.BiFunction<io.rsocket.Payload, Flux<io.rsocket.Payload>, org.reactivestreams.Publisher<? extends io.rsocket.Payload>>() {
      @Override
      public org.reactivestreams.Publisher<io.rsocket.Payload> apply(io.rsocket.Payload payload, Flux<io.rsocket.Payload> publisher) {
        return requestChannel(payload, publisher);
      }
    });
  }

  private static final java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload> serializer =
    new java.util.function.Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
      @Override
      public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
        int length = message.getSerializedSize();
        io.netty.buffer.ByteBuf byteBuf = io.netty.buffer.ByteBufAllocator.DEFAULT.buffer(length);
        try {
          message.writeTo(com.google.protobuf.CodedOutputStream.newInstance(byteBuf.internalNioBuffer(0, length)));
          byteBuf.writerIndex(length);
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
