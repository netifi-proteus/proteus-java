package io.netifi.proteus.presence;

import io.netifi.proteus.balancer.LoadBalancedRSocketSupplier;
import io.netifi.proteus.frames.DestinationAvailResult;
import io.netifi.proteus.frames.RouteDestinationFlyweight;
import io.netifi.proteus.frames.RouteType;
import io.netifi.proteus.frames.RoutingFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.collection.LongObjectHashMap;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Implementation of {@link PresenceNotifier} */
public class DefaultPresenceNotifier implements PresenceNotifier {
  private static final Logger logger = LoggerFactory.getLogger(DefaultPresenceNotifier.class);
  private static final Object EVENT = new Object();
  final LongObjectHashMap<ConcurrentHashMap<String, Set<String>>> presenceInfo;
  final ConcurrentHashMap<String, Disposable> watchingSubscriptions;
  private final TimebasedIdGenerator idGenerator;
  private final ReplayProcessor<Object> onChange;
  private final long fromAccessKey;
  private final String fromDestination;
  private final LoadBalancedRSocketSupplier rSocketSupplier;

  public DefaultPresenceNotifier(
      TimebasedIdGenerator idGenerator,
      long fromAccessKey,
      String fromDestination,
      LoadBalancedRSocketSupplier rSocketSupplier) {
    this.onChange = ReplayProcessor.cacheLast();
    this.idGenerator = idGenerator;
    this.fromAccessKey = fromAccessKey;
    this.fromDestination = fromDestination;
    this.presenceInfo = new LongObjectHashMap<>();
    this.watchingSubscriptions = new ConcurrentHashMap<>();
    this.rSocketSupplier = rSocketSupplier;

    rSocketSupplier
        .onClose()
        .doFinally(
            s -> {
              watchingSubscriptions.values().forEach(Disposable::dispose);
              watchingSubscriptions.clear();
              presenceInfo.clear();
            })
        .subscribe();
  }

  synchronized ConcurrentHashMap<String, Set<String>> getGroupMap(long accountId) {
    return presenceInfo.computeIfAbsent(accountId, a -> new ConcurrentHashMap<>());
  }

  public void watch(long accountId, String group) {
    if (logger.isTraceEnabled()) {
      logger.trace("watching account id {} and group {}", accountId, group);
    }

    ConcurrentHashMap<String, Set<String>> groupMap = getGroupMap(accountId);
    watchingSubscriptions.computeIfAbsent(
        groupKey(accountId, group),
        k -> {
          logger.debug("creating new watcher account id {} and group {}", accountId, group);
          ByteBuf route = createGroupRoute(RouteType.PRESENCE_GROUP_QUERY, accountId, group);
          ByteBuf routingInformation = createRoutingInformation(route, fromDestination);
          Payload payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, routingInformation);

          return rSocketSupplier
              .get()
              .requestStream(payload)
              .transform(flux -> handleResult(flux, groupMap, accountId, group, k))
              .subscribe();
        });
  }

  private Flux<Payload> handleResult(
      Flux<Payload> result,
      ConcurrentHashMap<String, Set<String>> groupMap,
      long accountId,
      String group,
      String mapKey) {
    return result
        .doOnNext(
            p -> {
              try {
                ByteBuf metadata = p.sliceMetadata();
                boolean found = DestinationAvailResult.found(metadata);
                String destination = DestinationAvailResult.destination(metadata);
                logger.debug(
                    "account id {}, destination {}, and group {} available = {}",
                    accountId,
                    destination,
                    group,
                    found);
                if (found) {
                  Set<String> destinations =
                      groupMap.computeIfAbsent(group, g -> ConcurrentHashMap.newKeySet());
                  destinations.add(destination);
                } else {
                  Set<String> destinations = groupMap.get(group);
                  if (destinations != null) {
                    destinations.remove(destination);
                    if (destinations.isEmpty()) {
                      groupMap.remove(group);
                    }
                  }
                }

                onChange.onNext(EVENT);
              } finally {
                p.release();
              }
            })
        .doFinally(
            s -> {
              logger.debug("removing watcher for account id {} and group {}", accountId, group);
              watchingSubscriptions.remove(mapKey);
            });
  }

  public void stopWatching(long accountId, String group) {
    Disposable disposable = watchingSubscriptions.get(groupKey(accountId, group));
    if (disposable != null) {
      disposable.dispose();
    }
  }

  public void watch(long accountId, String destination, String group) {
    logger.debug(
        "watching account id {}, group {}, and destination {}", accountId, group, destination);
    ConcurrentHashMap<String, Set<String>> groupMap = getGroupMap(accountId);

    watchingSubscriptions.computeIfAbsent(
        destinationkey(accountId, destination, group),
        k -> {
          ByteBuf route =
              createDestinationRoute(RouteType.PRESENCE_ID_QUERY, accountId, destination, group);
          ByteBuf routingInformation = createRoutingInformation(route, fromDestination);
          Payload payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, routingInformation);

          return rSocketSupplier
              .get()
              .requestStream(payload)
              .transform(flux -> handleResult(flux, groupMap, accountId, group, k))
              .subscribe();
        });
  }

  public void stopWatching(long accountId, String destination, String group) {
    Disposable disposable =
        watchingSubscriptions.get(destinationkey(accountId, destination, group));
    if (disposable != null) {
      disposable.dispose();
    }
  }

  boolean contains(long accountId, String group) {
    ConcurrentHashMap<String, Set<String>> groupMap = getGroupMap(accountId);

    return groupMap.containsKey(group);
  }

  boolean contains(long accountId, String destination, String group) {
    ConcurrentHashMap<String, Set<String>> groupMap = getGroupMap(accountId);

    Set<String> destinations = groupMap.get(group);

    if (destinations == null) {
      return false;
    } else {
      return destinations.contains(destination);
    }
  }

  @SuppressWarnings("unchecked")
  public Mono<Void> notify(long accountId, String group) {
    logger.trace("looking for account id {} and group {}", accountId, group);
    if (!watchingSubscriptions.contains(groupKey(accountId, group))) {
      watch(accountId, group);
    }

    if (contains(accountId, group)) {
      return Mono.empty();
    } else {
      return onChange
          .filter(o -> contains(accountId, group))
          .next()
          .then()
          .doOnError(
              throwable ->
                  logger.error(
                      "error waiting for notification for " + accountId + " and group " + group,
                      throwable));
    }
  }

  @SuppressWarnings("unchecked")
  public Mono<Void> notify(long accountId, String destination, String group) {
    logger.trace(
        "looking for account id {}, destination {}, and group {}", accountId, destination, group);
    if (!watchingSubscriptions.contains(destinationkey(accountId, destination, group))) {
      watch(accountId, destination, group);
    }

    if (contains(accountId, destination, group)) {
      return Mono.empty();
    } else {
      return onChange
          .filter(o -> contains(accountId, destination, group))
          .next()
          .then()
          .doOnError(
              throwable ->
                  logger.error(
                      "error waiting for notification for " + accountId + " and group " + group,
                      throwable));
    }
  }

  private String groupKey(long accountId, String group) {
    return accountId + ":" + group;
  }

  private String destinationkey(long accountId, String destination, String group) {
    return accountId + ":" + destination + ":" + group;
  }

  private ByteBuf createGroupRoute(RouteType routeType, long accountId, String group) {
    int length = RouteDestinationFlyweight.computeLength(routeType, group);
    byte[] bytes = new byte[length];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
    RouteDestinationFlyweight.encodeRouteByGroup(byteBuf, routeType, accountId, group);

    return byteBuf;
  }

  private ByteBuf createDestinationRoute(
      RouteType routeType, long accountId, String destination, String group) {
    int length = RouteDestinationFlyweight.computeLength(routeType, destination, group);
    byte[] bytes = new byte[length];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
    RouteDestinationFlyweight.encodeRouteByDestination(
        byteBuf, routeType, accountId, destination, group);

    return byteBuf;
  }

  private ByteBuf createRoutingInformation(ByteBuf route, String fromDestination) {
    int length = RoutingFlyweight.computeLength(false, fromDestination, route);
    byte[] bytes = new byte[length];
    ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

    RoutingFlyweight.encode(
        byteBuf, false, 1, fromAccessKey, fromDestination, idGenerator.nextId(), route);

    return byteBuf;
  }
}
