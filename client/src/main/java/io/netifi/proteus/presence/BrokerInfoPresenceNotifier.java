package io.netifi.proteus.presence;

import com.google.protobuf.Empty;
import io.netifi.proteus.broker.info.*;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BrokerInfoPresenceNotifier implements PresenceNotifier {
  private static final Logger logger = LoggerFactory.getLogger(BrokerInfoPresenceNotifier.class);
  FluxProcessor<Destination, Destination> joinEvents;
  FluxProcessor<ServiceEventResponse, ServiceEventResponse> serviceEvents;
  ConcurrentHashMap<String, ConcurrentHashMap<String, Destination>> groups;
  ConcurrentHashMap<String, Set<String>> services;
  private BrokerInfoService client;
  private ConcurrentMap<String, Disposable> groupWatches;
  private ConcurrentMap<String, ConcurrentMap<String, Disposable>> destinationWatches;
  private ConcurrentHashMap<String, Disposable> serviceWatches;

  public BrokerInfoPresenceNotifier(BrokerInfoService client) {
    this.client = client;
    this.groups = new ConcurrentHashMap<>();
    this.groupWatches = new ConcurrentHashMap<>();
    this.destinationWatches = new ConcurrentHashMap<>();
    this.services = new ConcurrentHashMap<>();
    this.joinEvents = DirectProcessor.create();
    this.serviceEvents = DirectProcessor.create();
  }

  @Override
  public void watch(String group) {
    Objects.requireNonNull(group);
    groupWatches.computeIfAbsent(
        group,
        g ->
            client
                .streamGroupEvents(
                    Group.newBuilder().setGroup(group).build(), Unpooled.EMPTY_BUFFER)
                .doFinally(
                    s -> {
                      synchronized (BrokerInfoPresenceNotifier.class) {
                        groups.clear();
                        services.clear();
                      }
                    })
                .retry()
                .subscribe(this::joinEvent));
  }

  @Override
  public void stopWatching(String group) {
    Disposable disposable = groupWatches.remove(group);
    if (disposable != null && !disposable.isDisposed()) {
      disposable.dispose();
    }
  }

  @Override
  public void watch(String destination, String group) {
    Map<String, Disposable> disposables =
        destinationWatches.computeIfAbsent(group, g -> new ConcurrentHashMap<>());
    disposables.computeIfAbsent(
        group,
        g ->
            client
                .streamDestinationEvents(
                    Destination.newBuilder().setDestination(destination).setGroup(group).build(),
                    Unpooled.EMPTY_BUFFER)
                .doFinally(s -> remove(group, destination))
                .retry()
                .subscribe(BrokerInfoPresenceNotifier.this::joinEvent));
  }

  @Override
  public void stopWatching(String destination, String group) {
    Map<String, Disposable> disposables = destinationWatches.get(group);
    if (disposables != null) {
      Disposable disposable = disposables.remove(destination);
      if (disposable != null && !disposable.isDisposed()) {
        disposable.dispose();
      }

      if (disposables.isEmpty()) {
        destinationWatches.remove(group);
      }
    }
  }

  private synchronized void remove(String group, String destination) {
    logger.info("removing group {} and destination {}", group, destination);
    ConcurrentHashMap<String, Destination> map = groups.get(group);
    if (map != null) {
      Destination removed = map.remove(destination);
      if (map.isEmpty()) {
        groups.remove(group);

        if (removed != null) {
          for (String service : removed.getServicesList()) {
            Set<String> set = services.get(service);
            if (set != null) {
              set.remove(service);
              if (set.isEmpty()) {
                services.remove(service);
              }
            }
          }
        }
      }
    }
  }

  private synchronized void removeService(String service) {}

  private synchronized boolean contains(String group) {
    return groups.containsKey(group);
  }

  private synchronized boolean contains(String group, String destination) {
    ConcurrentHashMap<String, Destination> destinations = groups.get(group);
    return destinations != null && destinations.containsKey(destination);
  }

  private synchronized boolean containsService(String service) {
    return services.contains(service);
  }

  private synchronized boolean containsService(String service, String group) {
    return contains(group) && services.contains(service);
  }

  private synchronized boolean containsService(String service, String group, String destination) {
    return containsService(service, group, destination) && services.contains(service);
  }

  private void serviceEvent(ServiceEventResponse event) {
    logger.info("presence notifier received service event {}", event.toString());
    synchronized (this) {
      services
          .computeIfAbsent(event.getService(), s -> ConcurrentHashMap.newKeySet())
          .add(event.getGroup());
    }
  }

  private void joinEvent(Event event) {
    Destination destination = event.getDestination();
    logger.info("presence notifier received event {}", event.toString());
    switch (event.getType()) {
      case JOIN:
        synchronized (this) {
          String group = destination.getGroup();
          groups
              .computeIfAbsent(group, g -> new ConcurrentHashMap<>())
              .put(destination.getDestination(), destination);

          for (String serivce : destination.getServicesList()) {
            services.computeIfAbsent(serivce, s -> ConcurrentHashMap.newKeySet()).add(group);
          }
        }
        if (joinEvents.hasDownstreams()) {
          joinEvents.onNext(destination);
        }
        break;
      case SERVICE_ADD:
        {
        }
        break;
      case SERVICE_REMOVE:
        {
        }
        break;
      case LEAVE:
        remove(destination.getGroup(), destination.getDestination());
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getType());
    }
  }

  private Flux<Destination> joinEventsStream(String group) {
    watch(group);

    return joinEvents.filter(info -> info.getGroup().equals(group));
  }

  private Flux<Destination> joinEventsStream(String destination, String group) {
    watch(destination, group);

    return joinEvents.filter(
        info -> info.getGroup().equals(group) && info.getDestination().equals(destination));
  }

  private Flux<ServiceEventResponse> serviceEventStream(String service) {
    watchService(service);

    return serviceEvents.filter(event -> event.equals(service));
  }

  @Override
  public Mono<Void> notify(String group) {
    Objects.requireNonNull(group);

    if (contains(group)) {
      return Mono.empty();
    } else {
      watch(group);

      return joinEventsStream(group).next().then();
    }
  }

  @Override
  public Mono<Void> notify(String destination, String group) {
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);

    if (contains(destination, group)) {
      return Mono.empty();
    } else {
      return joinEventsStream(destination, group).next().then();
    }
  }

  @Override
  public Mono<Void> notifyService(String service) {
    Objects.requireNonNull(service);
    return null;
  }

  @Override
  public Mono<Void> notifyService(String service, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(group);
    return null;
  }

  @Override
  public Mono<Void> notifyService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    return null;
  }

  @Override
  public void watchService(String service) {
    Objects.requireNonNull(service);

    serviceWatches.computeIfAbsent(
        service,
        s ->
            client
                .streamServiceEvents(Empty.getDefaultInstance(), Unpooled.EMPTY_BUFFER)
                .doFinally(f -> removeService(service))
                .retry()
                .subscribe(BrokerInfoPresenceNotifier.this::serviceEvent));
  }

  @Override
  public void watchService(String service, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(group);

    watch(group);
    watchService(service);
  }

  @Override
  public void watchService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);

    watch(destination, group);
    watchService(service);
  }

  @Override
  public void stopWatchingService(String service) {
    Objects.requireNonNull(service);
    Disposable remove = serviceWatches.remove(service);
    if (remove != null && !remove.isDisposed()) {
      remove.dispose();
    }
  }

  @Override
  public void stopWatchingService(String service, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(group);
    stopWatchingService(service);
  }

  @Override
  public void stopWatchingService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    stopWatchingService(service);
  }

  @Override
  public Mono<Void> registerService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    return client
        .registerService(
            ServiceRegistrationRequest.newBuilder()
                .setService(service)
                .setDestination(destination)
                .setGroup(group)
                .build(),
            Unpooled.EMPTY_BUFFER)
        .then();
  }
}
