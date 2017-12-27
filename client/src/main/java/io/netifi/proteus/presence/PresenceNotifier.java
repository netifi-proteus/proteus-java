package io.netifi.proteus.presence;

import reactor.core.publisher.Mono;

public interface PresenceNotifier {
  void watch(long accountId, String group);

  void stopWatching(long accountId, String group);

  void watch(long accountId, String destination, String group);

  void stopWatching(long accountId, String destination, String group);

  Mono<Void> notify(long accountId, String group);

  Mono<Void> notify(long accountId, String destination, String group);
}
