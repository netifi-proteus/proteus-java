package io.netifi.proteus.rpc;

import io.netifi.proteus.tracing.ProteusTracing;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Publisher;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.7.16-SNAPSHOT)",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
@io.netifi.proteus.annotations.internal.ProteusGenerated(
    type = io.netifi.proteus.annotations.internal.ProteusResourceType.CLIENT,
    idlClass = SimpleService.class)
public final class SimpleServiceClient_backup implements SimpleService {
  private final io.rsocket.RSocket rSocket;
  private final io.opentracing.Tracer tracer;
  private final Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>> requestReply;
  private final Function<? super Publisher<Void>, ? extends Publisher<Void>> fireAndForget;
  private final Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>> requestStream;
  private final Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>> streamingRequestSingleResponse;
  private final Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>> streamingRequestAndResponse;
  
  private final Function<Map<String, String>, Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>>> requestReplyTrace;
  private final Function<Map<String, String>, Function<? super Publisher<Void>, ? extends Publisher<Void>>> fireAndForgetTrace;
  private final Function<Map<String, String>, Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>>> requestStreamTrace;
  private final Function<Map<String, String>, Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>>> streamingRequestSingleResponseTrace;
  private final Function<Map<String, String>, Function<? super Publisher<SimpleResponse>, ? extends Publisher<SimpleResponse>>> streamingRequestAndResponseTrace;
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket) {
    this.rSocket = rSocket;
    this.tracer = null;
    this.requestReply = Function.identity();
    this.fireAndForget = Function.identity();
    this.requestStream = Function.identity();
    this.streamingRequestSingleResponse = Function.identity();
    this.streamingRequestAndResponse = Function.identity();
    
    this.requestReplyTrace = ProteusTracing.trace();
    this.fireAndForgetTrace = ProteusTracing.trace();
    this.requestStreamTrace = ProteusTracing.trace();
    this.streamingRequestSingleResponseTrace = ProteusTracing.trace();
    this.streamingRequestAndResponseTrace = ProteusTracing.trace();
  }
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket, io.opentracing.Tracer tracer) {
    this.rSocket = rSocket;
    this.tracer = tracer;
    this.requestReply = Function.identity();
    this.fireAndForget = Function.identity();
    this.requestStream = Function.identity();
    this.streamingRequestSingleResponse = Function.identity();
    this.streamingRequestAndResponse = Function.identity();
    this.requestReplyTrace =
        ProteusTracing.trace(
            tracer, SimpleService.METHOD_REQUEST_REPLY, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.fireAndForgetTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_FIRE_AND_FORGET, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.requestStreamTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_REQUEST_STREAM, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.streamingRequestSingleResponseTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.streamingRequestAndResponseTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
  }
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry) {
    this.rSocket = rSocket;
    this.tracer = null;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_REQUEST_REPLY);
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_FIRE_AND_FORGET);
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_REQUEST_STREAM);
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE);
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE);
    this.requestReplyTrace = ProteusTracing.trace();
    this.fireAndForgetTrace = ProteusTracing.trace();
    this.requestStreamTrace = ProteusTracing.trace();
    this.streamingRequestSingleResponseTrace = ProteusTracing.trace();
    this.streamingRequestAndResponseTrace = ProteusTracing.trace();
  }
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry, io.opentracing.Tracer tracer) {
    this.rSocket = rSocket;
    this.tracer = tracer;
    this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_REQUEST_REPLY);
    this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_FIRE_AND_FORGET);
    this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_REQUEST_STREAM);
    this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE);
    this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry, "proteus.client", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE);
    this.requestReplyTrace =
        ProteusTracing.trace(
            tracer, SimpleService.METHOD_REQUEST_REPLY, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE));
    this.fireAndForgetTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_FIRE_AND_FORGET, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.requestStreamTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_REQUEST_STREAM, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.streamingRequestSingleResponseTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
    this.streamingRequestAndResponseTrace = ProteusTracing.trace(tracer, SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE, io.netifi.proteus.tracing.Tag.of("proteus.service", SimpleService.SERVICE), io.netifi.proteus.tracing.Tag.of("proteus.type", "client"));
  }

  public reactor.core.publisher.Mono<SimpleResponse> requestReply(SimpleRequest message) {
    return requestReply(message, Unpooled.EMPTY_BUFFER);
  }

  @Override
  @io.netifi.proteus.annotations.internal.ProteusGeneratedMethod(returnTypeClass = SimpleResponse.class)
  public reactor.core.publisher.Mono<SimpleResponse> requestReply(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    Map<String, String> map = new HashMap<>();
    return reactor.core.publisher.Mono.defer(new Supplier<reactor.core.publisher.Mono<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Mono<io.rsocket.Payload> get() {
        final io.netty.buffer.ByteBuf tracingMetadata = ProteusTracing.mapToByteBuf(io.netty.buffer.ByteBufAllocator.DEFAULT, map);
        final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.SERVICE, SimpleService.METHOD_REQUEST_REPLY, tracingMetadata, metadata);
        tracingMetadata.release();
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestResponse(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(SimpleResponse.parser())).transform(requestReply).transform(requestReplyTrace.apply(map));
  }

  public reactor.core.publisher.Mono<Void> fireAndForget(SimpleRequest message) {
    return fireAndForget(message, Unpooled.EMPTY_BUFFER);
  }

  @Override
  @io.netifi.proteus.annotations.internal.ProteusGeneratedMethod(returnTypeClass = io.netifi.proteus.Empty.class)
  public reactor.core.publisher.Mono<Void> fireAndForget(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    Map<String, String> map = new HashMap<>();
    return reactor.core.publisher.Mono.defer(new Supplier<reactor.core.publisher.Mono<Void>>() {
      @Override
      public reactor.core.publisher.Mono<Void> get() {
        final io.netty.buffer.ByteBuf tracingMetadata = ProteusTracing.mapToByteBuf(io.netty.buffer.ByteBufAllocator.DEFAULT, map);
        final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.SERVICE, SimpleService.METHOD_FIRE_AND_FORGET, tracingMetadata, metadata);
        tracingMetadata.release();
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.fireAndForget(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).transform(fireAndForget).transform(fireAndForgetTrace.apply(map));
  }

  public reactor.core.publisher.Flux<SimpleResponse> requestStream(SimpleRequest message) {
    return requestStream(message, Unpooled.EMPTY_BUFFER);
  }

  @Override
  @io.netifi.proteus.annotations.internal.ProteusGeneratedMethod(returnTypeClass = SimpleResponse.class)
  public reactor.core.publisher.Flux<SimpleResponse> requestStream(SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    Map<String, String> map = new HashMap<>();
    return reactor.core.publisher.Flux.defer(new Supplier<reactor.core.publisher.Flux<io.rsocket.Payload>>() {
      @Override
      public reactor.core.publisher.Flux<io.rsocket.Payload> get() {
        final io.netty.buffer.ByteBuf tracingMetadata = ProteusTracing.mapToByteBuf(io.netty.buffer.ByteBufAllocator.DEFAULT, map);
        final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.SERVICE, SimpleService.METHOD_REQUEST_STREAM, tracingMetadata, metadata);
        tracingMetadata.release();
        io.netty.buffer.ByteBuf data = serialize(message);
        return rSocket.requestStream(io.rsocket.util.ByteBufPayload.create(data, metadataBuf));
      }
    }).map(deserializer(SimpleResponse.parser())).transform(requestStream).transform(requestStreamTrace.apply(map));
  }

  public reactor.core.publisher.Mono<SimpleResponse> streamingRequestSingleResponse(Publisher<SimpleRequest> messages) {
    return streamingRequestSingleResponse(messages, Unpooled.EMPTY_BUFFER);
  }

  @Override
  @io.netifi.proteus.annotations.internal.ProteusGeneratedMethod(returnTypeClass = SimpleResponse.class)
  public reactor.core.publisher.Mono<SimpleResponse> streamingRequestSingleResponse(Publisher<SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    Map<String, String> map = new HashMap<>();
    return rSocket.requestChannel(reactor.core.publisher.Flux.from(messages).map(
      new Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        private final java.util.concurrent.atomic.AtomicBoolean once = new java.util.concurrent.atomic.AtomicBoolean(false);

        @Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (once.compareAndSet(false, true)) {
            final io.netty.buffer.ByteBuf tracingMetadata = ProteusTracing.mapToByteBuf(io.netty.buffer.ByteBufAllocator.DEFAULT, map);
            final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.SERVICE, SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE, tracingMetadata, metadata);
            tracingMetadata.release();
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(SimpleResponse.parser())).single().transform(streamingRequestSingleResponse).transform(streamingRequestSingleResponseTrace.apply(map));
  }

  public reactor.core.publisher.Flux<SimpleResponse> streamingRequestAndResponse(Publisher<SimpleRequest> messages) {
    return streamingRequestAndResponse(messages, Unpooled.EMPTY_BUFFER);
  }

  @Override
  @io.netifi.proteus.annotations.internal.ProteusGeneratedMethod(returnTypeClass = SimpleResponse.class)
  public reactor.core.publisher.Flux<SimpleResponse> streamingRequestAndResponse(Publisher<SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    Map<String, String> map = new HashMap<>();
    return rSocket.requestChannel(reactor.core.publisher.Flux.from(messages).map(
      new Function<com.google.protobuf.MessageLite, io.rsocket.Payload>() {
        private final java.util.concurrent.atomic.AtomicBoolean once = new java.util.concurrent.atomic.AtomicBoolean(false);

        @Override
        public io.rsocket.Payload apply(com.google.protobuf.MessageLite message) {
          io.netty.buffer.ByteBuf data = serialize(message);
          if (once.compareAndSet(false, true)) {
            final io.netty.buffer.ByteBuf tracingMetadata = ProteusTracing.mapToByteBuf(io.netty.buffer.ByteBufAllocator.DEFAULT, map);
            final io.netty.buffer.ByteBuf metadataBuf = io.netifi.proteus.frames.ProteusMetadata.encode(io.netty.buffer.ByteBufAllocator.DEFAULT, SimpleService.SERVICE, SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE, tracingMetadata, metadata);
            tracingMetadata.release();
            return io.rsocket.util.ByteBufPayload.create(data, metadataBuf);
          } else {
            return io.rsocket.util.ByteBufPayload.create(data);
          }
        }
      })).map(deserializer(SimpleResponse.parser())).transform(streamingRequestAndResponse).transform(streamingRequestAndResponseTrace.apply(map));
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

  private static <T> Function<io.rsocket.Payload, T> deserializer(final com.google.protobuf.Parser<T> parser) {
    return new Function<io.rsocket.Payload, T>() {
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
