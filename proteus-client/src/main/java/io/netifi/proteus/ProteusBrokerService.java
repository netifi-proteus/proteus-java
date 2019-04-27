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
package io.netifi.proteus;

import io.netifi.proteus.common.tags.Tags;
import io.netifi.proteus.frames.BroadcastFlyweight;
import io.netifi.proteus.frames.GroupFlyweight;
import io.netifi.proteus.frames.ShardFlyweight;
import io.netifi.proteus.rsocket.DefaultProteusSocket;
import io.netifi.proteus.rsocket.ProteusSocket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;

interface ProteusBrokerService {
  default ProteusSocket group(CharSequence group, Tags tags) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              GroupFlyweight.encode(ByteBufAllocator.DEFAULT, group, metadataToWrap, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  default ProteusSocket broadcast(CharSequence group, Tags tags) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              BroadcastFlyweight.encode(ByteBufAllocator.DEFAULT, group, metadataToWrap, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  default ProteusSocket shard(CharSequence group, ByteBuf shardKey, Tags tags) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              ShardFlyweight.encode(
                  ByteBufAllocator.DEFAULT, group, metadataToWrap, shardKey, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  RSocket selectRSocket();
}
