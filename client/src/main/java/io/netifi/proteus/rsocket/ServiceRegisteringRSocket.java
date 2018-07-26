package io.netifi.proteus.rsocket;

import io.netifi.proteus.ProteusService;
import io.netifi.proteus.presence.PresenceNotifier;
import io.rsocket.Payload;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.function.Supplier;

public class ServiceRegisteringRSocket extends RSocketProxy implements ProteusService {
  private static final Logger logger = LoggerFactory.getLogger(ServiceRegisteringRSocket.class);
  private static final long CONNECTION_ATTEMPT_RESET_TS = Duration.ofMinutes(1).toMillis();
  private final CancellationException cancellationException =
      new CancellationException("connection has been closed");
  private final PresenceNotifier presenceNotifier;
  private final String service;
  private final String destination;
  private final String group;
  private final MonoProcessor<Void> onClose;
  private MonoProcessor<Void> currentOnRegistered;
  private boolean registering;
  private long lastConnectionAttemptTs = System.currentTimeMillis();
  private long attempts;
  private Supplier<Mono<Void>> onReconnectSupplier;

  private ServiceRegisteringRSocket(
      ProteusService source,
      PresenceNotifier presenceNotifier,
      String destination,
      String group,
      Supplier<Mono<Void>> onReconnectSupplier) {
    super(source);
    this.presenceNotifier = presenceNotifier;
    this.service = source.getService();
    this.destination = destination;
    this.onClose = MonoProcessor.create();
    this.group = group;
    this.onReconnectSupplier = onReconnectSupplier;

    resetOnRegistered();

    register();
  }

  public static ServiceRegisteringRSocket wrap(
      ProteusService source,
      PresenceNotifier presenceNotifier,
      String destination,
      String group,
      Supplier<Mono<Void>> onReconnectSupplier) {
    return new ServiceRegisteringRSocket(
        source, presenceNotifier, destination, group, onReconnectSupplier);
  }

  private synchronized void resetOnRegistered() {
    currentOnRegistered = MonoProcessor.create();
  }

  private synchronized Duration calculateRetryDuration() {
    long currentTs = System.currentTimeMillis();
    long oldTs = lastConnectionAttemptTs;
    long calculatedDuration = Math.min(attempts, 30);

    if (currentTs - oldTs > CONNECTION_ATTEMPT_RESET_TS) {
      attempts = 0;
    }

    lastConnectionAttemptTs = currentTs;

    attempts++;

    return Duration.ofSeconds(calculatedDuration);
  }

  void register() {
    synchronized (this) {
      if (registering) {
        return;
      }

      registering = true;
    }

    Mono.defer(() -> Mono.delay(calculateRetryDuration()).then(doRegister())).retry().subscribe();
  }

  private Mono<Void> doRegister() {
    return Mono.defer(
        () -> {
          if (source.isDisposed()) {
            return Mono.empty();
          }

          return presenceNotifier
              .registerService(service, destination, group)
              .doOnSuccess(
                  v -> {
                    synchronized (ServiceRegisteringRSocket.this) {
                      registering = false;
                    }
                    
                    onReconnectSupplier
                        .get()
                        .doFinally(s -> register())
                        .subscribe();
  
                    onRegistered().onComplete();
                  });
        });
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return onRegistered().then(super.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return onRegistered().then(super.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return onRegistered().thenMany(super.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return onRegistered().thenMany(super.requestChannel(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return onRegistered().then(super.metadataPush(payload));
  }

  @Override
  public String getService() {
    return service;
  }

  @Override
  public Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher) {
    return ((ProteusService) source).requestChannel(payload, publisher);
  }

  private synchronized MonoProcessor<Void> onRegistered() {
    return currentOnRegistered;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
}
