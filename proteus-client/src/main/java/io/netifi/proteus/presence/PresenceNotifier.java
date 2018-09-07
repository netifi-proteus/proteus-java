package io.netifi.proteus.presence;

import reactor.core.publisher.Mono;

/**
 * Notifies a caller when a destination or group is available. This is used by the {@link
 * io.netifi.proteus.rsocket.PresenceAwareRSocket} to wrap calls to ensure that requests are only
 * set to available resources. This could also be used to notify a user when something is available.
 * For instance if the desitnation of a client was tied to user id, you could use teo to see when
 * that user is connected.
 */
public interface PresenceNotifier {
  void watch(String group);

  void stopWatching(String group);

  void watch(String destination, String group);

  void stopWatching(String destination, String group);

  Mono<Void> notify(String group);

  Mono<Void> notify(String destination, String group);
}
