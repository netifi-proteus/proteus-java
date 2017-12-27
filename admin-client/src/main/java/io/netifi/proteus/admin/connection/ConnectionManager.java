package io.netifi.proteus.admin.connection;

import io.netifi.proteus.admin.rs.AdminRSocket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ConnectionManager {
  /**
   * Returns a single connection to a router
   *
   * @return
   */
  Mono<AdminRSocket> getRSocket();

  /**
   * Streams all active connections
   *
   * @return
   */
  Flux<AdminRSocket> getRSockets();
}
