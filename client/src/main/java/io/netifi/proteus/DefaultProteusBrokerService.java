package io.netifi.proteus;

import io.netifi.proteus.broker.info.Broker;
import io.netifi.proteus.broker.info.BrokerInfoServiceClient;
import io.netifi.proteus.broker.info.Event;
import io.netifi.proteus.frames.DestinationFlyweight;
import io.netifi.proteus.frames.DestinationSetupFlyweight;
import io.netifi.proteus.frames.GroupFlyweight;
import io.netifi.proteus.presence.BrokerInfoPresenceNotifier;
import io.netifi.proteus.presence.PresenceNotifier;
import io.netifi.proteus.rsocket.*;
import io.netifi.proteus.rsocket.transport.WeightedClientTransportSupplier;
import io.netifi.proteus.stats.FrugalQuantile;
import io.netifi.proteus.stats.Quantile;
import io.netifi.proteus.util.Xoroshiro128PlusRandom;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.MonoProcessor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class DefaultProteusBrokerService implements ProteusBrokerService, Disposable {
  private static final Logger logger = LoggerFactory.getLogger(DefaultProteusBrokerService.class);
  private static final double DEFAULT_EXP_FACTOR = 4.0;
  private static final double DEFAULT_LOWER_QUANTILE = 0.2;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int DEFAULT_INACTIVITY_FACTOR = 500;
  private static final int EFFORT = 5;
  final Quantile lowerQuantile = new FrugalQuantile(DEFAULT_LOWER_QUANTILE);
  final Quantile higherQuantile = new FrugalQuantile(DEFAULT_HIGHER_QUANTILE);
  private final List<SocketAddress> seedAddresses;
  private final List<WeightedClientTransportSupplier> suppliers;
  private final List<WeightedReconnectingRSocket> members;
  private final RSocket requestHandlingRSocket;
  private final Xoroshiro128PlusRandom rnd = new Xoroshiro128PlusRandom(System.nanoTime());
  private final String group;
  private final DestinationNameFactory destinationNameFactory;
  private final boolean keepalive;
  private final long tickPeriodSeconds;
  private final long ackTimeoutSeconds;
  private final int missedAcks;
  private final long accessKey;
  private final ByteBuf accessToken;
  private final Function<SocketAddress, ClientTransport> clientTransportFactory;
  private final int poolSize = Runtime.getRuntime().availableProcessors();
  private final double expFactor = DEFAULT_EXP_FACTOR;
  private final int inactivityFactor = DEFAULT_INACTIVITY_FACTOR;
  private final BrokerInfoServiceClient client;
  private final PresenceNotifier presenceNotifier;
  private final MonoProcessor<Void> onClose;
  private boolean clientTransportMissed = false;
  private boolean rsocketMissed = false;

  public DefaultProteusBrokerService(
      List<SocketAddress> seedAddresses,
      RequestHandlingRSocket requestHandlingRSocket,
      String group,
      DestinationNameFactory destinationNameFactory,
      Function<SocketAddress, ClientTransport> clientTransportFactory,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      ByteBuf accessToken) {
    Objects.requireNonNull(seedAddresses);
    if (seedAddresses.isEmpty()) {
      throw new IllegalStateException("seedAddress is empty");
    }

    Objects.requireNonNull(accessToken);
    if (accessToken.readableBytes() == 0) {
      throw new IllegalStateException("access token has no readable bytes");
    }

    Objects.requireNonNull(clientTransportFactory);

    this.seedAddresses = seedAddresses;
    this.requestHandlingRSocket = new UnwrappingRSocket(requestHandlingRSocket);
    this.group = group;
    this.destinationNameFactory = destinationNameFactory;
    this.members = new ArrayList<>();
    this.suppliers = new ArrayList<>();
    this.clientTransportFactory = clientTransportFactory;
    this.keepalive = keepalive;
    this.tickPeriodSeconds = tickPeriodSeconds;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
    this.missedAcks = missedAcks;
    this.accessKey = accessKey;
    this.accessToken = accessToken;
    this.onClose = MonoProcessor.create();

    seedClientTransportSupplier();

    createFirstConnection();

    this.client = new BrokerInfoServiceClient(unwrappedGroup("com.netifi.proteus.brokerServices"));
    this.presenceNotifier = new BrokerInfoPresenceNotifier(client);

    Disposable disposable =
        client
            .streamBrokerEvents(Empty.getDefaultInstance())
            .doOnSubscribe(s -> createRemainingConnections())
            .doOnNext(this::handleBrokerEvent)
            .doOnError(t -> logger.error("error streaming broker events", t))
            .retry()
            .subscribe();

    onClose
        .doFinally(
            s -> {
              disposable.dispose();
            })
        .subscribe();
  }

  BrokerInfoServiceClient getBrokerInfoServiceClient() {
    return client;
  }

  PresenceNotifier getBrokerInfoPresenceNotifier() {
    return presenceNotifier;
  }

  void seedClientTransportSupplier() {
    seedAddresses
        .stream()
        .map(address -> new WeightedClientTransportSupplier(address, clientTransportFactory))
        .forEach(suppliers::add);
  }

  void createFirstConnection() {
    WeightedReconnectingRSocket weightedReconnectingRSocket = createWeightedReconnectingRSocket();
    members.add(weightedReconnectingRSocket);
  }

  synchronized void createRemainingConnections() {
    while (members.size() < poolSize) {
      rsocketMissed = true;
      WeightedReconnectingRSocket rSocket = createWeightedReconnectingRSocket();
      members.add(rSocket);
    }
  }

  void handleBrokerEvent(Event event) {
    logger.info("received broker event {}", event.toString());
    Broker broker = event.getBroker();
    InetSocketAddress address =
        InetSocketAddress.createUnresolved(broker.getIpAddress(), broker.getPort());
    switch (event.getType()) {
      case JOIN:
        handleJoinEvent(address);
        break;
      case LEAVE:
        handleLeaveEvent(address);
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getType());
    }
  }

  synchronized void handleJoinEvent(InetSocketAddress address) {
    boolean found = false;
    for (WeightedClientTransportSupplier s : suppliers) {
      if (s.getSocketAddress().equals(address)) {
        found = true;
        break;
      }
    }

    if (!found) {
      logger.info("adding connection to {}", address);
      WeightedClientTransportSupplier s =
          new WeightedClientTransportSupplier(address, clientTransportFactory);

      clientTransportMissed = true;
      suppliers.add(s);
    }
  }

  synchronized void handleLeaveEvent(InetSocketAddress address) {
    Iterator<WeightedClientTransportSupplier> iterator = suppliers.iterator();

    while (iterator.hasNext()) {
      WeightedClientTransportSupplier next = iterator.next();
      if (next.getSocketAddress().equals(address)) {
        logger.info("removing connection to {}", address);
        iterator.remove();
        next.dispose();
        clientTransportMissed = true;
        break;
      }
    }
  }

  WeightedReconnectingRSocket createWeightedReconnectingRSocket() {
    return WeightedReconnectingRSocket.newInstance(
        requestHandlingRSocket,
        destinationNameFactory,
        this::getSetupPayload,
        this::isDisposed,
        this::selectClientTransportSupplier,
        keepalive,
        tickPeriodSeconds,
        ackTimeoutSeconds,
        missedAcks,
        accessKey,
        accessToken,
        lowerQuantile,
        higherQuantile,
        inactivityFactor);
  }

  Payload getSetupPayload(String computedFromDestination) {
    ByteBuf metadata =
        DestinationSetupFlyweight.encode(
            ByteBufAllocator.DEFAULT, computedFromDestination, group, accessKey, accessToken);
    return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, metadata);
  }

  private ProteusSocket unwrappedDestination(String destination, String group) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              DestinationFlyweight.encode(
                  ByteBufAllocator.DEFAULT,
                  DefaultProteusBrokerService.this.destinationNameFactory.peek(),
                  DefaultProteusBrokerService.this.group,
                  destination,
                  group,
                  metadataToWrap);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  private ProteusSocket unwrappedGroup(String group) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              GroupFlyweight.encode(
                  ByteBufAllocator.DEFAULT,
                  DefaultProteusBrokerService.this.destinationNameFactory.peek(),
                  DefaultProteusBrokerService.this.group,
                  group,
                  metadataToWrap);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  @Override
  public ProteusSocket destination(String destination, String group) {
    return PresenceAwareRSocket.wrap(unwrappedDestination(destination, group), destination, group, presenceNotifier);
  }

  @Override
  public ProteusSocket group(String group) {
    return PresenceAwareRSocket.wrap(unwrappedGroup(group), null, group, presenceNotifier);
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  private RSocket selectRSocket() {
    RSocket rSocket;
    List<WeightedReconnectingRSocket> _m;
    for (; ; ) {
      synchronized (this) {
        rsocketMissed = false;
        _m = members;
      }
      int size = _m.size();
      if (size == 1) {
        rSocket = _m.get(0);
      } else {
        WeightedReconnectingRSocket rsc1 = null;
        WeightedReconnectingRSocket rsc2 = null;

        for (int i = 0; i < EFFORT; i++) {
          int i1;
          int i2;
          synchronized (this) {
            i1 = rnd.nextInt(size);
            i2 = rnd.nextInt(size - 1);
          }
          if (i2 >= i1) {
            i2++;
          }
          rsc1 = _m.get(i1);
          rsc2 = _m.get(i2);
          if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
            break;
          }
        }

        double w1 = algorithmicWeight(rsc1);
        double w2 = algorithmicWeight(rsc2);
        if (w1 < w2) {
          rSocket = rsc2;
        } else {
          rSocket = rsc1;
        }
      }

      synchronized (this) {
        if (!rsocketMissed) {
          break;
        }
      }
    }

    return rSocket;
  }

  double algorithmicWeight(WeightedRSocket socket) {
    if (socket == null || socket.availability() == 0.0) {
      return 0.0;
    }
    int pendings = socket.pending();
    double latency = socket.predictedLatency();

    double low = lowerQuantile.estimation();
    double high =
        Math.max(
            higherQuantile.estimation(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      latency /= calculateFactor(low, latency, bandWidth);
    } else if (latency > high) {
      latency *= calculateFactor(latency, high, bandWidth);
    }

    return socket.availability() * 1.0 / (1.0 + latency * (pendings + 1));
  }

  private double calculateFactor(double u, double l, double bandWidth) {
    double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, expFactor);
  }

  private WeightedClientTransportSupplier selectClientTransportSupplier() {
    WeightedClientTransportSupplier supplier;

    for (; ; ) {
      List<WeightedClientTransportSupplier> _s;
      synchronized (this) {
        clientTransportMissed = false;
        _s = suppliers;
      }

      int size = _s.size();
      if (size == 1) {
        supplier = _s.get(0);
      } else {
        WeightedClientTransportSupplier supplier1 = null;
        WeightedClientTransportSupplier supplier2 = null;

        int i1;
        int i2;
        synchronized (this) {
          i1 = rnd.nextInt(size);
          i2 = rnd.nextInt(size - 1);
        }
        if (i2 >= i1) {
          i2++;
        }

        supplier1 = _s.get(i1);
        supplier2 = _s.get(i2);

        double w1 = supplier1.weight();
        double w2 = supplier2.weight();

        supplier = w1 < w2 ? supplier2 : supplier1;
      }

      synchronized (this) {
        if (!clientTransportMissed) {
          break;
        }
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("selecting socket {} with weight {}", supplier.toString(), supplier.weight());
    }

    return supplier;
  }
}
