package io.netifi.proteus.rsocket;

import io.netifi.proteus.frames.DestinationFlyweight;
import io.netifi.proteus.frames.FrameHeaderFlyweight;
import io.netifi.proteus.frames.FrameType;
import io.netifi.proteus.frames.GroupFlyweight;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// Need to unwrap Proteus Messages
public class UnwrappingRSocket extends RSocketProxy {

  public UnwrappingRSocket(RSocket source) {
    super(source);
  }

  private Payload unwrap(Payload payload) {
    ByteBuf data = payload.sliceData();
    ByteBuf metadata = payload.sliceMetadata();
    ByteBuf unwrappedMetadata;
    FrameType frameType = FrameHeaderFlyweight.frameType(metadata);
    switch (frameType) {
      case DESTINATION:
        unwrappedMetadata = DestinationFlyweight.metadata(metadata);
        break;
      case GROUP:
        unwrappedMetadata = GroupFlyweight.metadata(metadata);
        break;
      default:
        throw new IllegalStateException("unknown frame type " + frameType);
    }

    Payload unwrappedPayload = ByteBufPayload.create(data.retain(), unwrappedMetadata.retain());
    payload.release();
    return unwrappedPayload;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return super.fireAndForget(unwrap(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return super.requestResponse(unwrap(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return super.requestStream(unwrap(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return super.requestChannel(Flux.from(payloads).map(this::unwrap));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return super.metadataPush(unwrap(payload));
  }
}
