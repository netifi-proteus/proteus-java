package io.netifi.proteus.rpc;

import io.netifi.proteus.tracing.ProteusTracing;
import io.netifi.proteus.tracing.Tag;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;

import java.util.function.Function;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.7.16-SNAPSHOT)",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
@io.netifi.proteus.annotations.internal.ProteusGenerated(
    type = io.netifi.proteus.annotations.internal.ProteusResourceType.SERVICE,
    idlClass = SimpleService.class)
@javax.inject.Named(
    value ="SimpleServiceServer")
public final class SimpleServiceServer_backup extends io.netifi.proteus.AbstractProteusService {
  private final SimpleService service;
  private final Tracer tracer;
  private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> requestReply;
  private final Function<? super Publisher<Void>, ? extends Publisher<Void>> fireAndForget;
  private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> requestStream;
  private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> streamingRequestSingleResponse;
  private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> streamingRequestAndResponse;
  
  private final Function<SpanContext, Function<? super Publisher<Payload>, ? extends Publisher<Payload>>> requestReplyTrace;
  private final Function<SpanContext, Function<? super Publisher<Void>, ? extends Publisher<Void>>> fireAndForgetTrace;
  private final Function<SpanContext, Function<? super Publisher<Payload>, ? extends Publisher<Payload>>> requestStreamTrace;
  private final Function<SpanContext, Function<? super Publisher<Payload>, ? extends Publisher<Payload>>> streamingRequestSingleResponseTrace;
  private final Function<SpanContext, Function<? super Publisher<Payload>, ? extends Publisher<Payload>>> streamingRequestAndResponseTrace;
  
  
  @javax.inject.Inject
  public SimpleServiceServer(SimpleService service,
                             java.util.Optional<io.micrometer.core.instrument.MeterRegistry> registry,
                             java.util.Optional<Tracer> tracer) {
    this.service = service;
    if (!registry.isPresent()) {
      this.requestReply = Function.identity();
      this.fireAndForget = Function.identity();
      this.requestStream = Function.identity();
      this.streamingRequestSingleResponse = Function.identity();
      this.streamingRequestAndResponse = Function.identity();
    } else {
      this.requestReply = io.netifi.proteus.metrics.ProteusMetrics.timed(registry.get(), "proteus.server", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_REQUEST_REPLY);
      this.fireAndForget = io.netifi.proteus.metrics.ProteusMetrics.timed(registry.get(), "proteus.server", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_FIRE_AND_FORGET);
      this.requestStream = io.netifi.proteus.metrics.ProteusMetrics.timed(registry.get(), "proteus.server", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_REQUEST_STREAM);
      this.streamingRequestSingleResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry.get(), "proteus.server", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE);
      this.streamingRequestAndResponse = io.netifi.proteus.metrics.ProteusMetrics.timed(registry.get(), "proteus.server", "service", SimpleService.SERVICE, "method", SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE);
    }
    if (!tracer.isPresent()) {
      this.tracer = null;
      this.requestReplyTrace = ProteusTracing.traceAsChild();
      this.fireAndForgetTrace = ProteusTracing.traceAsChild();
      this.requestStreamTrace = ProteusTracing.traceAsChild();
      this.streamingRequestSingleResponseTrace = ProteusTracing.traceAsChild();
      this.streamingRequestAndResponseTrace = ProteusTracing.traceAsChild();
   } else {
      this.tracer = tracer.get();
      this.requestReplyTrace =
          ProteusTracing.traceAsChild(
              tracer.get(), SimpleService.METHOD_REQUEST_REPLY, Tag.of("proteus.service", SimpleService.SERVICE), Tag.of("proteus.type", "server"));
      this.fireAndForgetTrace =
          ProteusTracing.traceAsChild(
              tracer.get(), SimpleService.METHOD_FIRE_AND_FORGET, Tag.of("proteus.service", SimpleService.SERVICE), Tag.of("proteus.type", "server"));
      this.requestStreamTrace =
          ProteusTracing.traceAsChild(
              tracer.get(), SimpleService.METHOD_REQUEST_STREAM, Tag.of("proteus.service", SimpleService.SERVICE), Tag.of("proteus.type", "server"));
      this.streamingRequestSingleResponseTrace =
          ProteusTracing.traceAsChild(
              tracer.get(), SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE, Tag.of("proteus.service", SimpleService.SERVICE), Tag.of("proteus.type", "server"));
      this.streamingRequestAndResponseTrace =
          ProteusTracing.traceAsChild(
              tracer.get(), SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE, Tag.of("proteus.service", SimpleService.SERVICE), Tag.of("proteus.type", "server"));
    }
  }

  @Override
  public String getService() {
    return SimpleService.SERVICE;
  }

  @Override
  public Class<?> getServiceClass() {
    return service.getClass();
  }

  @Override
  public reactor.core.publisher.Mono<Void> fireAndForget(Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      SpanContext spanContext = ProteusTracing.deserializeTracingMetadata(tracer, metadata);
      switch(io.netifi.proteus.frames.ProteusMetadata.getMethod(metadata)) {
        case SimpleService.METHOD_FIRE_AND_FORGET: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.fireAndForget(SimpleRequest.parseFrom(is), metadata).transform(fireAndForget).transform(fireAndForgetTrace.apply(spanContext));
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
  public reactor.core.publisher.Mono<Payload> requestResponse(Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      SpanContext spanContext = ProteusTracing.deserializeTracingMetadata(tracer, metadata);
      switch(io.netifi.proteus.frames.ProteusMetadata.getMethod(metadata)) {
        case SimpleService.METHOD_REQUEST_REPLY: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.requestReply(SimpleRequest.parseFrom(is), metadata).map(serializer).transform(requestReply).transform(requestReplyTrace.apply(spanContext));
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
  public reactor.core.publisher.Flux<Payload> requestStream(Payload payload) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      SpanContext spanContext = ProteusTracing.deserializeTracingMetadata(tracer, metadata);
      switch(io.netifi.proteus.frames.ProteusMetadata.getMethod(metadata)) {
        case SimpleService.METHOD_REQUEST_STREAM: {
          com.google.protobuf.CodedInputStream is = com.google.protobuf.CodedInputStream.newInstance(payload.getData());
          return service.requestStream(SimpleRequest.parseFrom(is), metadata).map(serializer).transform(requestStream).transform(requestStreamTrace.apply(spanContext));
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
  public reactor.core.publisher.Flux<Payload> requestChannel(Payload payload, reactor.core.publisher.Flux<Payload> publisher) {
    try {
      io.netty.buffer.ByteBuf metadata = payload.sliceMetadata();
      SpanContext spanContext = ProteusTracing.deserializeTracingMetadata(tracer, metadata);
      switch(io.netifi.proteus.frames.ProteusMetadata.getMethod(metadata)) {
        case SimpleService.METHOD_STREAMING_REQUEST_SINGLE_RESPONSE: {
          reactor.core.publisher.Flux<SimpleRequest> messages =
            publisher.map(deserializer(SimpleRequest.parser()));
          return service.streamingRequestSingleResponse(messages, metadata).map(serializer).transform(streamingRequestSingleResponse).transform(streamingRequestSingleResponseTrace.apply(spanContext)).flux();
        }
        case SimpleService.METHOD_STREAMING_REQUEST_AND_RESPONSE: {
          reactor.core.publisher.Flux<SimpleRequest> messages =
            publisher.map(deserializer(SimpleRequest.parser()));
          return service.streamingRequestAndResponse(messages, metadata).map(serializer).transform(streamingRequestAndResponse).transform(streamingRequestSingleResponseTrace.apply(spanContext));
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
  public reactor.core.publisher.Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new io.rsocket.internal.SwitchTransform<Payload, Payload>(payloads, new java.util.function.BiFunction<Payload, reactor.core.publisher.Flux<Payload>, Publisher<? extends Payload>>() {
      @Override
      public Publisher<Payload> apply(Payload payload, reactor.core.publisher.Flux<Payload> publisher) {
        return requestChannel(payload, publisher);
      }
    });
  }

  private static final Function<com.google.protobuf.MessageLite, Payload> serializer =
    new Function<com.google.protobuf.MessageLite, Payload>() {
      @Override
      public Payload apply(com.google.protobuf.MessageLite message) {
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

  private static <T> Function<Payload, T> deserializer(final com.google.protobuf.Parser<T> parser) {
    return new Function<Payload, T>() {
      @Override
      public T apply(Payload payload) {
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
