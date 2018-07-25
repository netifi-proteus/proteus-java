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

  private final String service;
  private final String destination;
  private final String group;
  private final PresenceNotifier presenceNotifier;
  private Type type;

  private PresenceAwareRSocket(
      RSocket source, String destination, String group, PresenceNotifier presenceNotifier) {
    this(source, null, destination, group, presenceNotifier);
  }

  private PresenceAwareRSocket(
      RSocket source,
      String service,
      String destination,
      String group,
      PresenceNotifier presenceNotifier) {
    super(source);
    this.service = service;
    this.destination = destination;
    this.group = group;
    this.presenceNotifier = presenceNotifier;
    this.type = getType();

    onClose()
        .doFinally(
            signalType -> {
              switch (type) {
                case DESTINATION:
                  presenceNotifier.stopWatching(destination, group);
                  break;
                case GROUP:
                  presenceNotifier.stopWatching(group);
                  break;
                case SERVICE:
                  presenceNotifier.stopWatchingService(service);
                  break;
                case SERVICE_GROUP:
                  presenceNotifier.stopWatchingService(service, group);
                  break;
                case SERVICE_DESTINATION:
                  presenceNotifier.stopWatchingService(service, destination, group);
                  break;
                default:
                  // should never get here
                  throw new IllegalStateException("unknown type " + type);
              }
            })
        .subscribe();
  }

  public static PresenceAwareRSocket wrap(
      RSocket source, String destination, String group, PresenceNotifier presenceNotifier) {
    return new PresenceAwareRSocket(source, destination, group, presenceNotifier);
  }

  public static PresenceAwareRSocket wrap(
      RSocket source,
      String service,
      String destination,
      String group,
      PresenceNotifier presenceNotifier) {
    return new PresenceAwareRSocket(source, service, destination, group, presenceNotifier);
  }

  private Type getType() {
    Type type;
    if (service != null) {
      if (destination != null) {
        type = Type.SERVICE_DESTINATION;
      } else if (group != null) {
        type = Type.SERVICE_GROUP;
      } else {
        type = Type.SERVICE;
      }
    } else {
      if (destination != null) {
        type = Type.SERVICE_DESTINATION;
      } else {
        type = Type.SERVICE_GROUP;
      }
    }
    return type;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return _notify().doOnError(t -> payload.release()).then(source.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return _notify().doOnError(t -> payload.release()).then(source.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return _notify().doOnError(t -> payload.release()).thenMany(source.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return _notify().thenMany(source.requestChannel(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return _notify().doOnError(t -> payload.release()).then(source.metadataPush(payload));
  }

  private Mono<Void> _notify() {
    return Mono.defer(
        () -> {
          switch (type) {
            case DESTINATION:
              return presenceNotifier.notify(destination, group);
            case GROUP:
              return presenceNotifier.notify(group);
            case SERVICE:
              return presenceNotifier.notifyService(service);
            case SERVICE_GROUP:
              return presenceNotifier.notifyService(service, group);
            case SERVICE_DESTINATION:
              return presenceNotifier.notifyService(service, destination, group);
            default:
              // should never get here
              throw new IllegalStateException("unknown type " + type);
          }
        });
  }

  private enum Type {
    GROUP,
    DESTINATION,
    SERVICE,
    SERVICE_GROUP,
    SERVICE_DESTINATION
  }
}
