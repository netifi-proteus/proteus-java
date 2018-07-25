package io.netifi.proteus.rsocket;

import io.netifi.proteus.presence.PresenceNotifier;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.concurrent.CancellationException;

public class ServiceRegisteringRSocket extends RSocketProxy {
  private static final Logger logger = LoggerFactory.getLogger(ServiceRegisteringRSocket.class);

  private final CancellationException cancellationException =
      new CancellationException("connection has been closed");
  private final MonoProcessor<Void> onRegistered;

  private ServiceRegisteringRSocket(
      RSocket source,
      PresenceNotifier presenceNotifier,
      String service,
      String destination,
      String group) {
    super(source);
    this.onRegistered = MonoProcessor.create();
    Disposable disposable =
        presenceNotifier
            .registerService(service, destination, group)
            .doOnSuccess(v -> onRegistered.onComplete())
            .doOnError(
                throwable ->
                    logger.error(
                        String.format(
                            "error registering service %s for group %s and destination %s",
                            service, group, destination),
                        throwable))
            .subscribe();

    source
        .onClose()
        .doFinally(
            s -> {
              if (!disposable.isDisposed()) {
                disposable.dispose();
              }

              if (!onRegistered.isTerminated()) {
                onRegistered.onError(cancellationException);
              }
            })
        .subscribe();
  }

  public static ServiceRegisteringRSocket wrap(
      RSocketProxy source,
      PresenceNotifier presenceNotifier,
      String service,
      String destination,
      String group) {
    return new ServiceRegisteringRSocket(source, presenceNotifier, service, destination, group);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return onRegistered.then(super.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return onRegistered.then(super.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return onRegistered.thenMany(super.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return onRegistered.thenMany(super.requestChannel(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return onRegistered.then(super.metadataPush(payload));
  }
}
