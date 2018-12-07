package io.netifi.proteus.rsocket;

import io.netifi.proteus.frames.*;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;

// Need to unwrap RSocketRpc Messages
public class UnwrappingRSocket extends AbstractUnwrappingRSocket {

  public UnwrappingRSocket(RSocket source) {
    super(source);
  }

  @Override
  protected Payload unwrap(Payload payload) {
    try {
      ByteBuf data = payload.sliceData();
      ByteBuf metadata = payload.sliceMetadata();
      ByteBuf unwrappedMetadata;
      FrameType frameType = FrameHeaderFlyweight.frameType(metadata);
      switch (frameType) {
        case GROUP:
          unwrappedMetadata = GroupFlyweight.metadata(metadata);
          break;
        case BROADCAST:
          unwrappedMetadata = BroadcastFlyweight.metadata(metadata);
          break;
        case SHARD:
          unwrappedMetadata = ShardFlyweight.metadata(metadata);
          break;
        default:
          throw new IllegalStateException("unknown frame type " + frameType);
      }

      return ByteBufPayload.create(data.retain(), unwrappedMetadata.retain());
    } finally {
      payload.release();
    }
  }
}
