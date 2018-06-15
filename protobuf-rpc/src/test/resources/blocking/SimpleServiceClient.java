package io.netifi.proteus.rpc.blocking;

import reactor.core.publisher.Flux;

@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.7.1-SNAPSHOT)",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
public final class SimpleServiceClient implements SimpleService {
  private io.netifi.proteus.rpc.SimpleServiceClient delegate;
  private java.time.Duration deafultTimeout = java.time.Duration.ofSeconds(30);
  
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket) {
    this.delegate = new io.netifi.proteus.rpc.SimpleServiceClient(rSocket);
  }
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry) {
    this.delegate = new io.netifi.proteus.rpc.SimpleServiceClient(rSocket, registry);
  }
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket, java.time.Duration deafultTimeout) {
    this.delegate = new io.netifi.proteus.rpc.SimpleServiceClient(rSocket);
    this.deafultTimeout = deafultTimeout;
  }
  
  public SimpleServiceClient(io.rsocket.RSocket rSocket, java.time.Duration deafultTimeout, io.micrometer.core.instrument.MeterRegistry registry) {
    this.delegate = new io.netifi.proteus.rpc.SimpleServiceClient(rSocket);
    this.deafultTimeout = deafultTimeout;
  }

  public io.netifi.proteus.rpc.SimpleResponse requestReply(io.netifi.proteus.rpc.SimpleRequest message) {
    return requestReply(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.rpc.SimpleResponse requestReply(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    return delegate.requestReply(message, metadata).block();
  }

  public void fireAndForget(io.netifi.proteus.rpc.SimpleRequest message) {
     fireAndForget(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public void fireAndForget(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
     delegate.fireAndForget(message, metadata).block();
  }

  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message) {
    return requestStream(message, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata) {
    Flux<io.netifi.proteus.rpc.SimpleResponse> stream = delegate.requestStream(message, metadata);
    return new io.netifi.proteus.BlockingIterable<>(stream, reactor.util.concurrent.Queues.SMALL_BUFFER_SIZE, reactor.util.concurrent.Queues.small());
  }

  public io.netifi.proteus.rpc.SimpleResponse streamingRequestSingleResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages) {
    return streamingRequestSingleResponse(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.rpc.SimpleResponse streamingRequestSingleResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    return delegate.streamingRequestSingleResponse(Flux.defer(()->Flux.fromIterable(messages)))
        .block();
  }

  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages) {
    return streamingRequestAndResponse(messages, io.netty.buffer.Unpooled.EMPTY_BUFFER);
  }

  @Override
  public io.netifi.proteus.BlockingIterable<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata) {
    Flux<io.netifi.proteus.rpc.SimpleResponse> stream = delegate.streamingRequestAndResponse(Flux.defer(()->Flux.fromIterable(messages)), metadata);
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
