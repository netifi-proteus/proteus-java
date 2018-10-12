package io.netifi.proteus;

import io.netifi.proteus.rsocket.ProteusSocket;
import io.netifi.proteus.tags.Tags;

interface ProteusBrokerService {
  ProteusSocket unicast(Tags tags);

  ProteusSocket broadcast(Tags tags);
}
