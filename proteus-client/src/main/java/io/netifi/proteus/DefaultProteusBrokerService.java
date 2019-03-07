/*
 *    Copyright 2019 The Proteus Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.netifi.proteus;

import com.google.protobuf.Empty;
import io.netifi.proteus.broker.info.Broker;
import io.netifi.proteus.broker.info.BrokerInfoServiceClient;
import io.netifi.proteus.broker.info.Event;
import io.netifi.proteus.broker.info.Id;
import io.netifi.proteus.common.net.HostAndPort;
import io.netifi.proteus.common.stats.FrugalQuantile;
import io.netifi.proteus.common.stats.Quantile;
import io.netifi.proteus.common.tags.Tags;
import io.netifi.proteus.discovery.DiscoveryStrategy;
import io.netifi.proteus.frames.*;
import io.netifi.proteus.rsocket.*;
import io.netifi.proteus.rsocket.transport.BrokerAddressSelectors;
import io.netifi.proteus.rsocket.transport.WeightedClientTransportSupplier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.opentracing.Tracer;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultProteusBrokerService implements ProteusBrokerService, Disposable {
  private static final Logger logger = LoggerFactory.getLogger(DefaultProteusBrokerService.class);
  private static final double DEFAULT_EXP_FACTOR = 4.0;
  private static final double DEFAULT_LOWER_QUANTILE = 0.5;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int DEFAULT_INACTIVITY_FACTOR = 500;
  private static final int EFFORT = 5;

  private final Quantile lowerQuantile = new FrugalQuantile(DEFAULT_LOWER_QUANTILE);
  private final Quantile higherQuantile = new FrugalQuantile(DEFAULT_HIGHER_QUANTILE);
  private final List<SocketAddress> seedAddresses;
  private final List<WeightedClientTransportSupplier> suppliers;
  private final List<WeightedReconnectingRSocket> members;
  private final RSocket requestHandlingRSocket;
  private final InetAddress localInetAddress;
  private final String group;
  private final boolean keepalive;
  private final long tickPeriodSeconds;
  private final long ackTimeoutSeconds;
  private final int missedAcks;
  private final long accessKey;
  private final ByteBuf accessToken;
  private final String connectionIdSeed;
  private final short additionalSetupFlags;
  private final Tags tags;
  private final List<ByteBuf> setupMetadata;

  private final Function<Broker, InetSocketAddress> addressSelector;
  private final Function<SocketAddress, ClientTransport> clientTransportFactory;
  private final int poolSize;
  private final double expFactor = DEFAULT_EXP_FACTOR;
  private final int inactivityFactor = DEFAULT_INACTIVITY_FACTOR;
  private final BrokerInfoServiceClient client;
  private final MonoProcessor<Void> onClose;
  private final int selectRefresh;
  private final DiscoveryStrategy discoveryStrategy;
  private int missed = 0;
  private volatile int poolCount = 0;
  private volatile Disposable disposable;

  public DefaultProteusBrokerService(
      List<SocketAddress> seedAddresses,
      RequestHandlingRSocket requestHandlingRSocket,
      InetAddress localInetAddress,
      String group,
      Function<Broker, InetSocketAddress> addressSelector,
      Function<SocketAddress, ClientTransport> clientTransportFactory,
      int poolSize,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      ByteBuf accessToken,
      String connectionIdSeed,
      short additionalSetupFlags,
      Tags tags,
      Tracer tracer,
      DiscoveryStrategy discoveryStrategy) {

    this.discoveryStrategy = discoveryStrategy;

    if (discoveryStrategy == null) {
      if (seedAddresses.isEmpty()) {
        throw new IllegalStateException("seedAddress is empty");
      } else {
        this.seedAddresses = seedAddresses;
      }
    } else {
      this.seedAddresses = new CopyOnWriteArrayList<>();
    }

    Objects.requireNonNull(accessToken);
    if (accessToken.readableBytes() == 0) {
      throw new IllegalStateException("access token has no readable bytes");
    }

    Objects.requireNonNull(clientTransportFactory);

    this.requestHandlingRSocket = new UnwrappingRSocket(requestHandlingRSocket);
    this.localInetAddress = localInetAddress;
    this.group = group;
    this.members = Collections.synchronizedList(new ArrayList<>());
    this.suppliers = Collections.synchronizedList(new ArrayList<>());
    this.addressSelector = addressSelector;
    this.clientTransportFactory = clientTransportFactory;
    this.poolSize = poolSize;
    this.selectRefresh = poolSize / 2;
    this.keepalive = keepalive;
    this.tickPeriodSeconds = tickPeriodSeconds;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
    this.missedAcks = missedAcks;
    this.accessKey = accessKey;
    this.accessToken = accessToken;
    this.connectionIdSeed = connectionIdSeed;
    this.additionalSetupFlags = additionalSetupFlags;
    this.tags = tags;
    this.setupMetadata = new ArrayList<>();
    this.onClose = MonoProcessor.create();

    if (discoveryStrategy != null) {
      logger.info("discovery strategy found using " + discoveryStrategy.getClass());
      useDiscoveryStrategy();
    }

    this.client =
        new BrokerInfoServiceClient(group("com.netifi.proteus.brokerServices", Tags.empty()));
    this.disposable = listenToBrokerEvents().subscribe();

    onClose
        .doFinally(
            s -> {
              if (disposable != null) {
                disposable.dispose();
              }
            })
        .subscribe();
  }

  private void useDiscoveryStrategy() {
    Mono<List<InetSocketAddress>> discoveryNodes =
        discoveryStrategy
            .discoverNodes()
            .flatMapIterable(Function.identity())
            .map(hostAndPort -> new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()))
            .collectList()
            .doOnNext(
                i -> {
                  synchronized (this) {
                    missed++;
                    seedAddresses.clear();
                    seedAddresses.addAll(i);
                  }
                })
            .doOnError(
                throwable ->
                    logger.error(
                        "error getting seed nodes using discovery strategy "
                            + discoveryStrategy.getClass(),
                        throwable));

    Disposable subscribe =
        discoveryNodes
            .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1), Duration.ofSeconds(30))
            .thenMany(
                Flux.interval(Duration.ofSeconds(10))
                    .onBackpressureDrop()
                    .concatMap(i -> discoveryNodes))
            .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1), Duration.ofSeconds(30))
            .subscribe();

    onClose.doFinally(s -> subscribe.dispose()).subscribe();
  }

  private Supplier<Payload> createSetupPayloadSupplier(String connectionIdSuffix) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      if (connectionIdSeed != null) {
        md.update(connectionIdSeed.getBytes());
      }
      md.update(connectionIdSuffix.getBytes());

      final ByteBuf metadata =
          DestinationSetupFlyweight.encode(
              ByteBufAllocator.DEFAULT,
              localInetAddress,
              group,
              accessKey,
              accessToken,
              ConnectionId.wrap(md.digest()),
              additionalSetupFlags,
              tags);
      // To release later
      this.setupMetadata.add(metadata);
      return () -> ByteBufPayload.create(Unpooled.EMPTY_BUFFER, Unpooled.copiedBuffer(metadata));
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException("Failed to build connection id", ex);
    }
  }

  private synchronized void reconcileSuppliers(Set<Broker> incomingBrokers) {
    if (!suppliers.isEmpty()) {
      Set<Broker> existingBrokers =
          suppliers
              .stream()
              .map(WeightedClientTransportSupplier::getBroker)
              .collect(Collectors.toSet());

      Set<Broker> remove = new HashSet<>(existingBrokers);
      remove.removeAll(incomingBrokers);
      Set<Broker> add = new HashSet<>(incomingBrokers);
      add.removeAll(existingBrokers);

      for (Broker broker : remove) {
        handleJoinEvent(broker);
      }

      for (Broker broker : add) {
        handleLeaveEvent(broker);
      }
    }
  }

  private Flux<Event> listenToBrokerEvents() {
    return Flux.defer(
            () ->
                client
                    .brokers(Empty.getDefaultInstance())
                    .collect(Collectors.toSet())
                    .doOnNext(this::reconcileSuppliers)
                    .thenMany(client.streamBrokerEvents(Empty.getDefaultInstance())))
        .doOnNext(this::handleBrokerEvent)
        .doOnError(
            t -> {
              logger.warn(
                  "error streaming broker events - make sure access key {} has a valid access token",
                  accessKey);
              logger.trace("error streaming broker events", t);
            })
        .onErrorResume(
            new Function<Throwable, Publisher<? extends Event>>() {
              long attempts = 0;
              long lastAttempt = System.currentTimeMillis();

              @Override
              public synchronized Publisher<? extends Event> apply(Throwable throwable) {
                if (Duration.ofMillis(System.currentTimeMillis() - lastAttempt).getSeconds() > 30) {
                  attempts = 0;
                }

                Mono<Event> then =
                    Mono.delay(Duration.ofMillis(attempts * 500)).then(Mono.error(throwable));
                if (attempts < 30) {
                  attempts++;
                }

                lastAttempt = System.currentTimeMillis();

                return then;
              }
            })
        .retry();
  }

  private void seedClientTransportSupplier() {
    synchronized (this) {
      missed++;
    }
    seedAddresses
        .stream()
        .map(
            address -> {
              try {
                Broker b;
                String str = address.toString();
                URI u = URI.create(str);
                switch (u.getScheme()) {
                  case "ws":
                  case "wss":
                    b =
                        Broker.newBuilder()
                            .setWebSocketAddress(u.getHost())
                            .setWebSocketPort(u.getPort())
                            .build();
                    return new WeightedClientTransportSupplier(
                        b, BrokerAddressSelectors.WEBSOCKET_ADDRESS, clientTransportFactory);
                  case "tcp":
                    b =
                        Broker.newBuilder()
                            .setTcpAddress(u.getHost())
                            .setTcpPort(u.getPort())
                            .build();
                    return new WeightedClientTransportSupplier(
                        b, BrokerAddressSelectors.TCP_ADDRESS, clientTransportFactory);
                  default:
                    // Assume URI is actually a HostAndPort, and TCP is our default
                    HostAndPort hostAndPort = HostAndPort.fromString(str);
                    b =
                        Broker.newBuilder()
                            .setTcpAddress(hostAndPort.getHost())
                            .setTcpPort(hostAndPort.getPort())
                            .build();
                    return new WeightedClientTransportSupplier(
                        b, BrokerAddressSelectors.TCP_ADDRESS, clientTransportFactory);
                }
              } catch (Throwable t) {
                InetSocketAddress address1 = (InetSocketAddress) address;

                logger.info("can't parse socket to URI");
                return new WeightedClientTransportSupplier(
                    Broker.newBuilder()
                        .setTcpAddress(address1.getHostName())
                        .setTcpPort(address1.getPort())
                        .build(),
                    BrokerAddressSelectors.TCP_ADDRESS,
                    clientTransportFactory);
              }
            })
        .forEach(suppliers::add);
  }

  private synchronized void handleBrokerEvent(Event event) {
    logger.info("received broker event {} - {}", event.getType(), event.toString());
    Broker broker = event.getBroker();
    switch (event.getType()) {
      case JOIN:
        handleJoinEvent(broker);
        break;
      case LEAVE:
        handleLeaveEvent(broker);
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getType());
    }
  }

  private void handleJoinEvent(Broker broker) {
    Id incomingBrokerId = broker.getBrokerId();
    Optional<WeightedClientTransportSupplier> first =
        suppliers
            .stream()
            .filter(
                supplier -> Objects.equals(supplier.getBroker().getBrokerId(), incomingBrokerId))
            .findAny();

    if (!first.isPresent()) {
      logger.info("adding transport supplier to broker {}", broker);

      WeightedClientTransportSupplier s =
          new WeightedClientTransportSupplier(broker, addressSelector, clientTransportFactory);
      suppliers.add(s);

      s.onClose()
          .doFinally(
              signalType -> {
                logger.info("removing transport supplier to broker {}", broker);
                suppliers.removeIf(
                    supplier -> supplier.getBroker().getBrokerId().equals(broker.getBrokerId()));
              })
          .subscribe();

      missed++;
      createConnection();
    }
  }

  private void handleLeaveEvent(Broker broker) {
    suppliers
        .stream()
        .filter(
            supplier -> Objects.equals(supplier.getBroker().getBrokerId(), broker.getBrokerId()))
        .findAny()
        .ifPresent(
            supplier -> {
              logger.info("removing transport supplier to {}", broker);
              supplier.dispose();
              missed++;
            });
  }

  private WeightedReconnectingRSocket createWeightedReconnectingRSocket() {
    return WeightedReconnectingRSocket.newInstance(
        requestHandlingRSocket,
        createSetupPayloadSupplier(String.valueOf(poolCount++)),
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

  @Override
  public ProteusSocket group(CharSequence group, Tags tags) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              GroupFlyweight.encode(ByteBufAllocator.DEFAULT, group, metadataToWrap, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  @Override
  public ProteusSocket broadcast(CharSequence group, Tags tags) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              BroadcastFlyweight.encode(ByteBufAllocator.DEFAULT, group, metadataToWrap, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  @Override
  public ProteusSocket shard(CharSequence group, ByteBuf shardKey, Tags tags) {
    return new DefaultProteusSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              ShardFlyweight.encode(
                  ByteBufAllocator.DEFAULT, group, metadataToWrap, shardKey, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  @Override
  public void dispose() {
    for (ByteBuf metadata : setupMetadata) {
      ReferenceCountUtil.safeRelease(metadata);
    }
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  private synchronized void createConnection() {
    if (members.size() < poolSize) {
      missed++;
      WeightedReconnectingRSocket rSocket = createWeightedReconnectingRSocket();
      members.add(rSocket);
    }
  }

  private RSocket selectRSocket() {
    RSocket rSocket;
    List<WeightedReconnectingRSocket> _m;
    int r;
    for (; ; ) {
      boolean createConnection;
      synchronized (this) {
        r = missed;
        _m = members;

        createConnection = members.size() < selectRefresh;
      }

      if (createConnection) {
        createConnection();
        continue;
      }

      int size = _m.size();
      if (size == 1) {
        rSocket = _m.get(0);
      } else {
        WeightedReconnectingRSocket rsc1 = null;
        WeightedReconnectingRSocket rsc2 = null;

        for (int i = 0; i < EFFORT; i++) {
          int i1 = ThreadLocalRandom.current().nextInt(size);
          int i2 = ThreadLocalRandom.current().nextInt(size - 1);

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
        if (r == missed) {
          break;
        }
      }
    }

    return rSocket;
  }

  private double algorithmicWeight(WeightedRSocket socket) {
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
    int c;
    for (; ; ) {
      boolean selectTransports;
      List<WeightedClientTransportSupplier> _s;
      synchronized (this) {
        c = missed;
        _s = suppliers;

        selectTransports = suppliers.isEmpty();
      }

      if (selectTransports) {
        seedClientTransportSupplier();
        continue;
      }

      int size = _s.size();
      if (size == 1) {
        supplier = _s.get(0);
      } else {
        WeightedClientTransportSupplier supplier1 = null;
        WeightedClientTransportSupplier supplier2 = null;

        int i1 = ThreadLocalRandom.current().nextInt(size);
        int i2 = ThreadLocalRandom.current().nextInt(size - 1);

        if (i2 >= i1) {
          i2++;
        }

        supplier1 = _s.get(i1);
        supplier2 = _s.get(i2);

        double w1 = supplier1.weight();
        double w2 = supplier2.weight();

        if (logger.isDebugEnabled()) {
          logger.debug("selecting candidate socket {} with weight {}", supplier1.toString(), w1);
          logger.debug("selecting candidate socket {} with weight {}", supplier2.toString(), w2);
        }

        supplier = w1 < w2 ? supplier1 : supplier2;
      }

      synchronized (this) {
        if (c == missed) {
          supplier.select();
          missed++;
          break;
        }
      }
    }

    logger.info("selected socket {}", supplier.toString());

    return supplier;
  }
}
