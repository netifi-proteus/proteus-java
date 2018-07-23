package io.netifi.proteus.presence;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import io.netifi.proteus.broker.info.*;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BrokerInfoPresenceNotifier implements PresenceNotifier {
  private static final Logger logger = LoggerFactory.getLogger(BrokerInfoPresenceNotifier.class);
  FluxProcessor<Destination, Destination> joinEvents;
  Table<String, String, Broker> groups;
  private BrokerInfoService client;
  private ConcurrentMap<String, Disposable> groupWatches;
  private ConcurrentMap<String, ConcurrentMap<String, Disposable>> destinationWatches;

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
                .doFinally(
                    s -> {
                      synchronized (BrokerInfoPresenceNotifier.class) {
                        List<String> strings = new ArrayList<>(groups.row(group).keySet());
                        for (String d : strings) {
                          remove(group, d);
                        }
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
    groups.remove(group, destination);
  }

  private synchronized boolean contains(String group) {
    return !groups.row(group).isEmpty();
  }

  private synchronized boolean contains(String group, String destination) {
    return groups.row(group).containsKey(destination);
  }

  private void joinEvent(Event event) {
    Destination destination = event.getDestination();
    logger.info("presence notifier received event {}", event.toString());
    switch (event.getType()) {
      case JOIN:
        groups.put(destination.getGroup(), destination.getDestination(), destination.getBroker());
        if (joinEvents.hasDownstreams()) {
          joinEvents.onNext(destination);
        }
        break;
      case LEAVE:
        remove(destination.getGroup(), destination.getDestination());
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getType());
    }
  }

  @Override
  public Mono<Void> notify(String group) {
    Objects.requireNonNull(group);

    if (contains(group)) {
      return Mono.empty();
    } else {
      watch(group);

      return joinEvents.filter(info -> info.getGroup().equals(group)).next().then();
    }
  }

  @Override
  public Mono<Void> notify(String destination, String group) {
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);

    if (contains(group, destination)) {
      return Mono.empty();
    } else {
      watch(destination, group);

      return joinEvents
          .filter(
              info -> info.getGroup().equals(group) && info.getDestination().equals(destination))
          .next()
          .then();
    }
  }
}
