package io.netifi.proteus;

import io.netifi.proteus.rsocket.ProteusSocket;

interface ProteusBrokerService {
  ProteusSocket destination(String destination, String group);

  ProteusSocket group(String group);

  ProteusSocket broadcast(String group);
}
