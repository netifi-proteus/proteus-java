package io.netifi.proteus.presence;

import io.netifi.proteus.balancer.LoadBalancedRSocketSupplier;
import io.netifi.proteus.frames.DestinationAvailResult;
import io.netifi.proteus.frames.RouteDestinationFlyweight;
import io.netifi.proteus.frames.RouteType;
import io.netifi.proteus.frames.RoutingFlyweight;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

/** Implementation of {@link PresenceNotifier} */
public class DefaultPresenceNotifier implements PresenceNotifier {
  private static final Logger logger = LoggerFactory.getLogger(DefaultPresenceNotifier.class);
  private static final byte[] EMPTY = new byte[0];
  private static final Object EVENT = new Object();
  final Set<PresenceNotificationInfo> presenceInfo;
  final ConcurrentHashMap<String, Disposable> watchingSubscriptions;
  private final TimebasedIdGenerator idGenerator;
  private final ReplayProcessor onChange;
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
    this.presenceInfo = ConcurrentHashMap.newKeySet();
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

  public void watch(long accountId, String group) {
    if (logger.isTraceEnabled()) {
      logger.trace("watching account id {} and group {}", accountId, group);
    }

    watchingSubscriptions.computeIfAbsent(
        groupKey(accountId, group),
        k -> {
          logger.debug("creating new watcher account id {} and group {}", accountId, group);
          ByteBuf route = createGroupRoute(RouteType.PRESENCE_GROUP_QUERY, accountId, group);
          ByteBuf routingInformation = createRoutingInformation(route, fromDestination);
          Payload payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, routingInformation);

          Disposable subscribe =
              rSocketSupplier
                  .get()
                  .requestStream(payload)
                  .doOnNext(
                      p -> {
                        try {
                          boolean found = DestinationAvailResult.found(p.sliceMetadata());

                          PresenceNotificationInfo presenceNotificationInfo =
                              new PresenceNotificationInfo(null, accountId, group);
                          if (found) {
                            logger.debug("account id {} and group {} available", accountId, group);
                            presenceInfo.add(presenceNotificationInfo);
                          } else {
                            logger.debug("account id {} and group {} available", accountId, group);
                            presenceInfo.remove(presenceNotificationInfo);
                          }

                          onChange.onNext(EMPTY);
                        } finally {
                          p.release();
                        }
                      })
                  .doFinally(
                      s -> {
                        logger.debug(
                            "removing watcher for account id {} and group {}", accountId, group);
                        watchingSubscriptions.remove(k);
                      })
                  .subscribe();

          return subscribe;
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
    watchingSubscriptions.computeIfAbsent(
        groupKey(accountId, group),
        k -> {
          ByteBuf route =
              createDestinationRoute(RouteType.PRESENCE_ID_QUERY, accountId, destination, group);
          ByteBuf routingInformation = createRoutingInformation(route, fromDestination);
          Payload payload = ByteBufPayload.create(Unpooled.EMPTY_BUFFER, routingInformation);

          Disposable subscribe =
              rSocketSupplier
                  .get()
                  .requestStream(payload)
                  .doOnNext(
                      p -> {
                        try {
                          boolean found = DestinationAvailResult.found(p.sliceMetadata());

                          PresenceNotificationInfo presenceNotificationInfo =
                              new PresenceNotificationInfo(destination, accountId, group);
                          if (found) {
                            presenceInfo.add(presenceNotificationInfo);
                          } else {
                            presenceInfo.remove(presenceNotificationInfo);
                          }
                          onChange.onNext(EVENT);
                        } finally {
                          p.release();
                        }
                      })
                  .doFinally(
                      s -> {
                        logger.debug(
                            "removing watcher for account id {}, group {} and destination {}",
                            accountId,
                            group,
                            destination);
                        watchingSubscriptions.remove(k);
                      })
                  .subscribe();

          return subscribe;
        });
  }

  public void stopWatching(long accountId, String destination, String group) {
    Disposable disposable =
        watchingSubscriptions.get(destinationkey(accountId, destination, group));
    if (disposable != null) {
      disposable.dispose();
    }
  }

  @SuppressWarnings("unchecked")
  public Mono<Void> notify(long accountId, String group) {
    logger.trace("looking for account id {} and group {}", accountId, group);
    if (!watchingSubscriptions.contains(groupKey(accountId, group))) {
      watch(accountId, group);
    }

    PresenceNotificationInfo info = new PresenceNotificationInfo(null, accountId, group);
    if (presenceInfo.contains(info)) {
      return Mono.empty();
    } else {
      return onChange
          .filter(o -> presenceInfo.contains(info))
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

    PresenceNotificationInfo info = new PresenceNotificationInfo(destination, accountId, group);
    if (presenceInfo.contains(info)) {
      return Mono.empty();
    } else {
      return onChange
          .filter(o -> presenceInfo.contains(info))
          .next()
          .then()
          .timeout(Duration.ofSeconds(5))
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

  class PresenceNotificationInfo
      implements DestinationPresenceNotificationInfo, GroupPresenceNotificationInfo {
    private String destination;
    private long accountId;
    private String group;

    PresenceNotificationInfo(String destination, long accountId, String group) {
      this.destination = destination;
      this.accountId = accountId;
      this.group = group;
    }

    public String getDestination() {
      return destination;
    }

    public long getAccountId() {
      return accountId;
    }

    public String getGroup() {
      return group;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PresenceNotificationInfo that = (PresenceNotificationInfo) o;

      if (accountId != that.accountId) return false;
      if (destination != null ? !destination.equals(that.destination) : that.destination != null)
        return false;
      return group != null ? group.equals(that.group) : that.group == null;
    }

    @Override
    public int hashCode() {
      int result = destination != null ? destination.hashCode() : 0;
      result = 31 * result + (int) (accountId ^ (accountId >>> 32));
      result = 31 * result + (group != null ? group.hashCode() : 0);
      return result;
    }
  }
}
