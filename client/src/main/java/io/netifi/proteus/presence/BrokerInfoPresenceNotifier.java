package io.netifi.proteus.presence;

import com.google.protobuf.StringValue;
import io.netifi.proteus.broker.info.*;
import io.netifi.proteus.collections.IndexableStore;
import io.netifi.proteus.collections.Object2ObjectHashMap;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Predicate;

public class BrokerInfoPresenceNotifier implements PresenceNotifier {
  private static final Logger logger = LoggerFactory.getLogger(BrokerInfoPresenceNotifier.class);
  private static final IndexableStore.KeyHasher<String> DESTINATION_HASHER =
      new IndexableStore.KeyHasher<String>() {
        @Override
        public long hash(String... t) {
          String group = t[0];
          String destination = t[1];
          return pack(group.hashCode(), destination.hashCode());
        }

        private long pack(int group, int destination) {
          return (((long) group) << 32) | (destination & 0xffffffffL);
        }
      };
  FluxProcessor<Destination, Destination> joinEvents;
  private Object2ObjectHashMap<WatchKey, Disposable> watches = new Object2ObjectHashMap<>();

  private BrokerInfoService client;

  IndexableStore<String, IndexableStore.KeyHasher<String>, Destination> store =
      new IndexableStore<>(DESTINATION_HASHER);

  public BrokerInfoPresenceNotifier(BrokerInfoService client) {
    this.client = client;
  }

  @Override
  public void watch(String group) {
    Objects.requireNonNull(group);
    watches.computeIfAbsent(
        WatchKey.of(null, null, group),
        k ->
            client
                .streamGroupEvents(
                    Group.newBuilder().setGroup(group).build(), Unpooled.EMPTY_BUFFER)
                .doFinally(s -> store.removeByQuery("group", group))
                .retry()
                .subscribe(this::joinEvent));
  }

  @Override
  public void watch(String destination, String group) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(destination);
    watches.computeIfAbsent(
        WatchKey.of(null, destination, group),
        k ->
            client
                .streamDestinationEvents(
                    Destination.newBuilder().setDestination(destination).setGroup(group).build(),
                    Unpooled.EMPTY_BUFFER)
                .doFinally(s -> store.remove(group, destination))
                .subscribe(this::joinEvent));
  }

  @Override
  public void watchService(String service) {
    Objects.requireNonNull(service);
    watches.computeIfAbsent(
        WatchKey.of(service, null, null),
        k ->
            client
                .streamServiceEvents(
                    StringValue.newBuilder().setValue(service).build(), Unpooled.EMPTY_BUFFER)
                .doFinally(s -> store.removeByQuery("service", service))
                .retry()
                .subscribe(this::joinEvent));
  }

  @Override
  public void watchService(String service, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(group);
    watches.computeIfAbsent(
        WatchKey.of(service, null, group),
        k ->
            client
                .streamGroupEvents(
                    Group.newBuilder().setGroup(group).build(), Unpooled.EMPTY_BUFFER)
                .filter(
                    event ->
                        event
                            .getDestination()
                            .getServicesList()
                            .stream()
                            .filter(Predicate.isEqual(service))
                            .findFirst()
                            .isPresent())
                .doFinally(s -> store.removeByQuery("service", service))
                .retry()
                .subscribe(this::joinEvent));
  }

  @Override
  public void watchService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    watches.computeIfAbsent(
        WatchKey.of(service, destination, group),
        k ->
            client
                .streamDestinationEvents(
                    Destination.newBuilder().setDestination(destination).setGroup(group).build(),
                    Unpooled.EMPTY_BUFFER)
                .filter(
                    event ->
                        event
                            .getDestination()
                            .getServicesList()
                            .stream()
                            .filter(Predicate.isEqual(service))
                            .findFirst()
                            .isPresent())
                .doFinally(s -> store.remove(group, destination))
                .retry()
                .subscribe(this::joinEvent));
  }

  @Override
  public void stopWatching(String group) {
    Objects.requireNonNull(group);
    Disposable remove = watches.remove(WatchKey.of(null, null, group));

    if (remove != null) {
      remove.dispose();
    }
  }

  @Override
  public void stopWatching(String destination, String group) {
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    Disposable remove = watches.remove(WatchKey.of(null, destination, group));

    if (remove != null) {
      remove.dispose();
    }
  }

  @Override
  public void stopWatchingService(String service) {
    Objects.requireNonNull(service);
    Disposable remove = watches.remove(WatchKey.of(service, null, null));

    if (remove != null) {
      remove.dispose();
    }
  }

  @Override
  public void stopWatchingService(String service, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(group);
    Disposable remove = watches.remove(WatchKey.of(service, null, group));

    if (remove != null) {
      remove.dispose();
    }
  }

  @Override
  public void stopWatchingService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    Disposable remove = watches.remove(WatchKey.of(service, destination, group));

    if (remove != null) {
      remove.dispose();
    }
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

  @Override
  public Mono<Void> notify(String group) {
    Objects.requireNonNull(group);

    if (store.containsTag("group", group)) {
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
    if (store.containsKey(group, destination)) {
      return Mono.empty();
    } else {
      watch(destination, group);
      return joinEventsStream(destination, group).next().then();
    }
  }

  @Override
  public Mono<Void> notifyService(String service) {
    Objects.requireNonNull(service);
    if (store.containsTag("service", service)) {
      return Mono.empty();
    } else {
      watch(service);
      return joinEvents
          .flatMapIterable(destination -> destination.getServicesList())
          .filter(Predicate.isEqual(service))
          .next()
          .then();
    }
  }

  @Override
  public Mono<Void> notifyService(String service, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(group);

    if (store.containsTags("service", service, "group", group)) {
      return Mono.empty();
    } else {
      watchService(service, group);
      return joinEventsStream(group)
          .flatMapIterable(destination -> destination.getServicesList())
          .filter(Predicate.isEqual(service))
          .next()
          .then();
    }
  }

  @Override
  public Mono<Void> notifyService(String service, String destination, String group) {
    Objects.requireNonNull(service);
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);

    if (store.containsTags("service", service, "destination", destination, "group", group)) {
      return Mono.empty();
    } else {
      watchService(service, destination, group);
      return joinEventsStream(group, destination)
          .flatMapIterable(Destination::getServicesList)
          .filter(Predicate.isEqual(service))
          .next()
          .then();
    }
  }

  private void joinEvent(Event event) {
    Destination destination = event.getDestination();
    logger.info("presence notifier received event {}", event.toString());
    switch (event.getType()) {
      case JOIN:
        synchronized (this) {
          IndexableStore.Entry<Destination> entry =
              store.put(destination, destination.getGroup(), destination.getDestination());
          entry.add("group", destination.getGroup());

          for (String service : destination.getServicesList()) {
            entry.add("service", service);
          }
        }

        if (joinEvents.hasDownstreams()) {
          joinEvents.onNext(destination);
        }
        break;
      case LEAVE:
        {
          store.remove(destination.getGroup(), destination.getDestination());
          break;
        }
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

  private static class WatchKey {
    String group;
    String service;
    String destination;

    private WatchKey(String service, String destination, String group) {
      this.group = group;
      this.service = service;
      this.destination = destination;
    }

    static WatchKey of(String service, String destination, String group) {
      return new WatchKey(service, destination, group);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WatchKey watchKey = (WatchKey) o;
      return Objects.equals(group, watchKey.group)
          && Objects.equals(service, watchKey.service)
          && Objects.equals(destination, watchKey.destination);
    }

    @Override
    public int hashCode() {
      return Objects.hash(group, service, destination);
    }
  }
}
