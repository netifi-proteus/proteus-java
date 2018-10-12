package io.netifi.proteus.rsocket;

import io.netifi.proteus.presence.PresenceNotifier;
import io.netifi.proteus.tags.Tags;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * RSocket implementation that uses the {@link PresenceNotifier} to determine if the group, or
 * destination is available before sending a request. It will wait in a non-blocking manner to
 * determine if the group, or destination is present.
 */
public class PresenceAwareRSocket extends RSocketProxy implements ProteusSocket {

  private final Tags tags;
  private final PresenceNotifier presenceNotifier;

  private PresenceAwareRSocket(RSocket source, Tags tags, PresenceNotifier presenceNotifier) {
    super(source);
    this.tags = tags;
    this.presenceNotifier = presenceNotifier;

    Disposable disposable = presenceNotifier.watch(tags);
    onClose().doFinally(signalType -> disposable.dispose()).subscribe();
  }

  public static PresenceAwareRSocket wrap(
      RSocket source, Tags tags, PresenceNotifier presenceNotifier) {
    return new PresenceAwareRSocket(source, tags, presenceNotifier);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return presenceNotifier
        .notify(tags)
        .doOnError(t -> payload.release())
        .then(source.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return presenceNotifier
        .notify(tags)
        .doOnError(t -> payload.release())
        .then(source.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return presenceNotifier
        .notify(tags)
        .doOnError(t -> payload.release())
        .thenMany(source.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return presenceNotifier.notify(tags).thenMany(source.requestChannel(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return presenceNotifier
        .notify(tags)
        .doOnError(t -> payload.release())
        .then(source.metadataPush(payload));
  }
}
