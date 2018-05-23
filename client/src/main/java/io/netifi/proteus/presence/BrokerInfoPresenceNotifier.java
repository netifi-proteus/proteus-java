package io.netifi.proteus.presence;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import io.netty.buffer.Unpooled;
import io.proteus.broker.info.*;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerInfoPresenceNotifier implements PresenceNotifier {
  FluxProcessor<Destination, Destination> joinEvents;
  private BrokerInfoService client;
  Table<String, String, Broker> groups;
  private Map<String, Disposable> groupWatches;
  private Map<String, Map<String, Disposable>> destinationWatches;

  public BrokerInfoPresenceNotifier(BrokerInfoService client) {
    this.client = client;
    this.groups = Tables.synchronizedTable(HashBasedTable.create());
    this.groupWatches = new ConcurrentHashMap<>();
    this.destinationWatches = new ConcurrentHashMap<>();
    this.joinEvents = DirectProcessor.create();
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

  private void joinEvent(Event event) {
    Destination destination = event.getDestination();
    switch (event.getEventType()) {
      case JOIN:
        groups.put(destination.getGroup(), destination.getDestination(), destination.getBroker());
        if (joinEvents.hasDownstreams()) {
          joinEvents.onNext(destination);
        }
        break;
      case LEAVE:
        groups.remove(destination.getGroup(), destination.getDestination());
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getEventType());
    }
  }

  @Override
  public Mono<Void> notify(String group) {
    Objects.requireNonNull(group);
    watch(group);

    Mono<Boolean> containsGroup = Mono.fromCallable(() -> groups.containsRow(group));
    Flux<Boolean> joinEvents =
        this.joinEvents.map(destination -> destination.getGroup().equals(group));

    return Flux.merge(containsGroup, joinEvents).filter(Boolean::booleanValue).take(1).then();
  }

  @Override
  public Mono<Void> notify(String destination, String group) {
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    watch(destination, group);

    Mono<Boolean> containsGroup = Mono.fromCallable(() ->
                                                        groups.contains(group, destination));
    Flux<Boolean> joinEvents =
        this.joinEvents.map(
            d -> d.getGroup().equals(group) && d.getDestination().equals(destination));

    return Flux.merge(containsGroup, joinEvents).filter(Boolean::booleanValue).take(1).then();
  }
}
