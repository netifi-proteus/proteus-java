package io.netifi.testing.protobuf;

/**
 * <pre>
 * A simple service for test.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.4.0)",
    comments = "Source: io.netifi.sdk.proteus/simpleservice.proto")
public interface SimpleService {
  int NAMESPACE_ID = 298608432;
  int SERVICE_ID = -1305494814;
  int METHOD_FIRE_AND_FORGET = 238626589;
  int METHOD_STREAM_ON_FIRE_AND_FORGET = -254431682;
  int METHOD_UNARY_RPC = -1434830019;
  int METHOD_CLIENT_STREAMING_RPC = 356703499;
  int METHOD_SERVER_STREAMING_RPC = -803409785;
  int METHOD_SERVER_STREAMING_FIRE_HOSE = 374837461;
  int METHOD_BIDI_STREAMING_RPC = -1207876110;

  /**
   * <pre>
   * fire and forget
   * </pre>
   */
  reactor.core.publisher.Mono<Void> fireAndForget(SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Streams when you send a Fire and Forget
   * </pre>
   */
  reactor.core.publisher.Flux<SimpleResponse> streamOnFireAndForget(Empty message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Simple unary RPC.
   * </pre>
   */
  reactor.core.publisher.Mono<SimpleResponse> unaryRpc(SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Simple client-to-server streaming RPC.
   * </pre>
   */
  reactor.core.publisher.Mono<SimpleResponse> clientStreamingRpc(org.reactivestreams.Publisher<SimpleRequest> messages, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Simple server-to-client streaming RPC.
   * </pre>
   */
  reactor.core.publisher.Flux<SimpleResponse> serverStreamingRpc(SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Simple server-to-client streaming RPC.
   * </pre>
   */
  reactor.core.publisher.Flux<SimpleResponse> serverStreamingFireHose(SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Simple bidirectional streaming RPC.
   * </pre>
   */
  reactor.core.publisher.Flux<SimpleResponse> bidiStreamingRpc(org.reactivestreams.Publisher<SimpleRequest> messages, io.netty.buffer.ByteBuf metadata);
}
