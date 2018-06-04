package io.netifi.proteus.rsocket;

import io.netifi.proteus.presence.PresenceNotifier;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * RSocket implementation that uses the {@link PresenceNotifier} to determine if the group, or
 * destination is available before sending a request. It will wait in a non-blocking manner to
 * determine if the group, or destination is present.
 */
public class PresenceAwareRSocket extends RSocketProxy implements ProteusSocket {

  private final String destination;
  private final String group;
  private final PresenceNotifier presenceNotifier;
  private final boolean groupRoute;

  private PresenceAwareRSocket(
      RSocket source,
      String destination,
      String group,
      PresenceNotifier presenceNotifier) {
    super(source);
    this.destination = destination;
    this.group = group;
    this.presenceNotifier = presenceNotifier;
    this.groupRoute = destination == null || destination.isEmpty();
    
    onClose()
        .doFinally(signalType -> {
          if (groupRoute) {
            presenceNotifier.stopWatching(group);
          } else {
            presenceNotifier.stopWatching(destination, group);
          }
        })
        .subscribe();
  }

  public static PresenceAwareRSocket wrap(
      RSocket source,
      String destination,
      String group,
      PresenceNotifier presenceNotifier) {
    return new PresenceAwareRSocket(source, destination, group, presenceNotifier);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return _notify()
        .doOnError(t -> payload.release())
        .then(source.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return _notify()
        .doOnError(t -> payload.release())
        .then(source.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return _notify()
        .doOnError(t -> payload.release())
        .thenMany(source.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return _notify()
        .thenMany(source.requestChannel(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return _notify()
        .doOnError(t -> payload.release())
        .then(source.metadataPush(payload));
  }

  private Mono<Void> _notify() {
    return Mono.defer(() -> {
      if (groupRoute) {
        return presenceNotifier.notify(group);
      } else {
        return presenceNotifier.notify(destination, group);
      }
    });
  }
  
}
