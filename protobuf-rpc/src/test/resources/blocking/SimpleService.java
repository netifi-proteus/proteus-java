package io.netifi.proteus.rpc.blocking;

/**
 */
@javax.annotation.Generated(
    value = "by Proteus proto compiler (version 0.7.1-SNAPSHOT)",
    comments = "Source: io/netifi/proteus/rpc/simpleservice.proto")
public interface SimpleService {
  int NAMESPACE_ID = 1249370865;
  int SERVICE_ID = -1305494814;
  int METHOD_REQUEST_REPLY = -1541595385;
  int METHOD_FIRE_AND_FORGET = 761533144;
  int METHOD_REQUEST_STREAM = -2146926651;
  int METHOD_STREAMING_REQUEST_SINGLE_RESPONSE = -152147254;
  int METHOD_STREAMING_REQUEST_AND_RESPONSE = 93731825;

  /**
   * <pre>
   * Request / Response
   * </pre>
   */
  io.netifi.proteus.rpc.SimpleResponse requestReply(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Fire-and-Forget
   * </pre>
   */
  void fireAndForget(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Single Request / Streaming Response
   * </pre>
   */
  Iterable<io.netifi.proteus.rpc.SimpleResponse> requestStream(io.netifi.proteus.rpc.SimpleRequest message, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Streaming Request / Single Response
   * </pre>
   */
  io.netifi.proteus.rpc.SimpleResponse streamingRequestSingleResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata);

  /**
   * <pre>
   * Streaming Request / Streaming Response
   * </pre>
   */
  Iterable<io.netifi.proteus.rpc.SimpleResponse> streamingRequestAndResponse(Iterable<io.netifi.proteus.rpc.SimpleRequest> messages, io.netty.buffer.ByteBuf metadata);
}
