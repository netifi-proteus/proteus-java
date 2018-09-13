package io.netifi.proteus;

import io.netifi.proteus.rsocket.ProteusSocket;
import reactor.core.Disposable;

interface ProteusBrokerService extends Disposable {
  ProteusSocket destination(String destination, String group);

  ProteusSocket group(String group);

  ProteusSocket broadcast(String group);
}
