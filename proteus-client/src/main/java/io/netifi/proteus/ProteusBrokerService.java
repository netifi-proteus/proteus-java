package io.netifi.proteus;

import io.micrometer.core.instrument.Tags;
import io.netifi.proteus.rsocket.ProteusSocket;
import io.netty.buffer.ByteBuf;

interface ProteusBrokerService {
  ProteusSocket group(CharSequence group, Tags tags);

  ProteusSocket broadcast(CharSequence group, Tags tags);

  ProteusSocket shard(CharSequence group, ByteBuf shardKey, Tags tags);
}
