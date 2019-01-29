/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
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
