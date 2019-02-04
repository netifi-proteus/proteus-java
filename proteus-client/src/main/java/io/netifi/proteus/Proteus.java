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

import io.netifi.proteus.broker.info.Broker;
import io.netifi.proteus.common.tags.Tag;
import io.netifi.proteus.common.tags.Tags;
import io.netifi.proteus.rsocket.NamedRSocketClientWrapper;
import io.netifi.proteus.rsocket.NamedRSocketServiceWrapper;
import io.netifi.proteus.rsocket.ProteusSocket;
import io.netifi.proteus.rsocket.transport.BrokerAddressSelectors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentracing.Tracer;
import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.rpc.RSocketRpcService;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.tcp.TcpClient;

/** This is where the magic happens */
public class Proteus implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(Proteus.class);
  private static final ConcurrentHashMap<String, Proteus> PROTEUS = new ConcurrentHashMap<>();

  static {
    // Set the Java DNS cache to 60 seconds
    java.security.Security.setProperty("networkaddress.cache.ttl", "60");
  }

  private final long accesskey;
  private final String group;
  private final Tags tags;
  private final ProteusBrokerService brokerService;
  private MonoProcessor<Void> onClose;
  private RequestHandlingRSocket requestHandlingRSocket;

  private Proteus(
      long accessKey,
      ByteBuf accessToken,
      InetAddress inetAddress,
      String group,
      Tags tags,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      List<SocketAddress> seedAddresses,
      Function<Broker, InetSocketAddress> addressSelector,
      Function<SocketAddress, ClientTransport> clientTransportFactory,
      int poolSize,
      Supplier<Tracer> tracerSupplier) {
    this.accesskey = accessKey;
    this.group = group;
    this.tags = tags;

    this.onClose = MonoProcessor.create();
    this.requestHandlingRSocket = new RequestHandlingRSocket();
    this.brokerService =
        new DefaultProteusBrokerService(
            seedAddresses,
            requestHandlingRSocket,
            inetAddress,
            group,
            clientTransportFactory,
            poolSize,
            keepalive,
            tickPeriodSeconds,
            ackTimeoutSeconds,
            missedAcks,
            accessKey,
            accessToken,
            tags,
            tracerSupplier.get());
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void dispose() {
    requestHandlingRSocket.dispose();
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

  /**
   * Adds an RSocketRpcService to be handle requests
   *
   * @param service the RSocketRpcService instance
   * @return current Proteus builder instance
   */
  public Proteus addService(RSocketRpcService service) {
    Objects.requireNonNull(service);
    requestHandlingRSocket.addService(service);
    return this;
  }

  /**
   * Adds an RSocket handler that will be located by name. This lets Proteus bridge raw RSocket
   * betweens services that don't use RSocketRpcService. It will route to a RSocket by specific
   * name, but it will give you a raw data so the implementor must deal with the incoming Payload.
   *
   * @param name the name of the RSocket
   * @param rSocket the RSocket to handle the requests
   * @return current Proteus builder instance
   */
  public Proteus addNamedRSocket(String name, RSocket rSocket) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(rSocket);
    return addService(NamedRSocketServiceWrapper.wrap(name, rSocket));
  }

  @Deprecated
  public ProteusSocket destination(String destination, String group) {
    return groupServiceSocket(group, Tags.of("destination", destination));
  }

  @Deprecated
  public ProteusSocket group(String group) {
    return groupServiceSocket(group, Tags.empty());
  }

  @Deprecated
  public ProteusSocket broadcast(String group) {
    return broadcastServiceSocket(group, Tags.empty());
  }

  @Deprecated
  public ProteusSocket shard(String group, ByteBuf shardKey) {
    return shardServiceSocket(group, shardKey, Tags.empty());
  }

  public ProteusSocket groupServiceSocket(String group, Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);
    return brokerService.group(group, tags);
  }

  public ProteusSocket broadcastServiceSocket(String group, Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);
    return brokerService.broadcast(group, tags);
  }

  public ProteusSocket shardServiceSocket(String group, ByteBuf shardKey, Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);
    return brokerService.shard(group, shardKey, tags);
  }

  public ProteusSocket groupNamedRSocket(String name, String group, Tags tags) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), groupServiceSocket(group, tags));
  }

  public ProteusSocket broadcastNamedRSocket(String name, String group, Tags tags) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), broadcastServiceSocket(group, tags));
  }

  public ProteusSocket shardNamedRSocket(String name, String group, ByteBuf shardKey, Tags tags) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), shardServiceSocket(group, shardKey, tags));
  }

  public long getAccesskey() {
    return accesskey;
  }

  public String getGroupName() {
    return group;
  }

  public Tags getTags() {
    return tags;
  }

  public static class Builder {
    private InetAddress inetAddress = DefaultBuilderConfig.getLocalAddress();
    private String host = DefaultBuilderConfig.getHost();
    private Integer port = DefaultBuilderConfig.getPort();
    private List<SocketAddress> seedAddresses = DefaultBuilderConfig.getSeedAddress();
    private Long accessKey = DefaultBuilderConfig.getAccessKey();
    private String group = DefaultBuilderConfig.getGroup();
    private String destination = DefaultBuilderConfig.getDestination();
    private Tags tags = DefaultBuilderConfig.getTags();
    private String accessToken = DefaultBuilderConfig.getAccessToken();
    private byte[] accessTokenBytes = new byte[20];
    private boolean sslDisabled = DefaultBuilderConfig.isSslDisabled();
    private boolean keepalive = DefaultBuilderConfig.getKeepAlive();
    private long tickPeriodSeconds = DefaultBuilderConfig.getTickPeriodSeconds();
    private long ackTimeoutSeconds = DefaultBuilderConfig.getAckTimeoutSeconds();
    private int missedAcks = DefaultBuilderConfig.getMissedAcks();
    private Function<Broker, InetSocketAddress> addressSelector =
        BrokerAddressSelectors.TCP_ADDRESS; // Default

    private Function<SocketAddress, ClientTransport> clientTransportFactory = null;
    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;
    private Supplier<Tracer> tracerSupplier = () -> null;

    public Builder clientTransportFactory(
        Function<SocketAddress, ClientTransport> clientTransportFactory) {
      this.clientTransportFactory = clientTransportFactory;
      return this;
    }

    public Builder poolSize(int poolSize) {
      this.poolSize = poolSize;
      return this;
    }

    public Builder sslDisabled(boolean sslDisabled) {
      this.sslDisabled = sslDisabled;
      return this;
    }

    public Builder keepalive(boolean useKeepAlive) {
      this.keepalive = useKeepAlive;
      return this;
    }

    public Builder tickPeriodSeconds(long tickPeriodSeconds) {
      this.tickPeriodSeconds = tickPeriodSeconds;
      return this;
    }

    public Builder ackTimeoutSeconds(long ackTimeoutSeconds) {
      this.ackTimeoutSeconds = ackTimeoutSeconds;
      return this;
    }

    public Builder missedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder tracerSupplier(Supplier<Tracer> tracerSupplier) {
      this.tracerSupplier = tracerSupplier;
      return this;
    }

    public Builder seedAddresses(Collection<SocketAddress> addresses) {
      if (addresses instanceof List) {
        this.seedAddresses = (List<SocketAddress>) addresses;
      } else {
        this.seedAddresses = new ArrayList<>(addresses);
      }

      return this;
    }

    private InetSocketAddress toInetSocketAddress(String address) {
      Objects.requireNonNull(address);
      String[] s = address.split(":");

      if (s.length != 2) {
        throw new IllegalArgumentException(address + " was a valid host address");
      }

      return InetSocketAddress.createUnresolved(s[0], Integer.parseInt(s[1]));
    }

    /**
     * Lets you add a strings in the form host:port
     *
     * @param address the first address to seed the broker with.
     * @param addresses additional addresses to seed the broker with.
     * @return the initial builder.
     */
    public Builder seedAddresses(String address, String... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(toInetSocketAddress(address));

      if (addresses != null) {
        for (String s : addresses) {
          list.add(toInetSocketAddress(address));
        }
      }

      return seedAddresses(list);
    }

    public Builder seedAddresses(SocketAddress address, SocketAddress... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(address);

      if (addresses != null) {
        list.addAll(Arrays.asList(addresses));
      }

      return seedAddresses(list);
    }

    public Builder localAddress(String address) {
      try {
        return localAddress(InetAddress.getByName(address));
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }

    public Builder localAddress(InetAddress address) {
      this.inetAddress = address;
      return this;
    }

    public Builder accessKey(long accessKey) {
      this.accessKey = accessKey;
      return this;
    }

    public Builder accessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    public Builder group(String group) {
      this.group = group;
      return this;
    }

    public Builder destination(String destination) {
      this.destination = destination;
      return this;
    }

    public Builder tag(String key, String value) {
      this.tags = tags.and(key, value);
      return this;
    }

    public Builder tags(String... tags) {
      this.tags = this.tags.and(tags);
      return this;
    }

    public Builder tags(Iterable<Tag> tags) {
      this.tags = this.tags.and(tags);
      return this;
    }

    public Builder addressSelector(Function<Broker, InetSocketAddress> addressSelector) {
      this.addressSelector = addressSelector;
      return this;
    }

    public Proteus build() {
      Objects.requireNonNull(accessKey, "account key is required");
      Objects.requireNonNull(accessToken, "account token is required");
      Objects.requireNonNull(group, "group is required");

      if (destination != null) {
        tags = tags.and("destination", destination);
      }

      if (clientTransportFactory == null) {
        logger.info("Client transport factory not provided; using TCP transport.");
        if (sslDisabled) {
          clientTransportFactory =
              address -> {
                TcpClient client = TcpClient.create().addressSupplier(() -> address);
                return TcpClientTransport.create(client);
              };
        } else {
          try {
            final SslProvider sslProvider;
            if (OpenSsl.isAvailable()) {
              logger.info("Native SSL provider is available; will use native provider.");
              sslProvider = SslProvider.OPENSSL_REFCNT;
            } else {
              logger.info("Native SSL provider not available; will use JDK SSL provider.");
              sslProvider = SslProvider.JDK;
            }
            final SslContext sslContext =
                SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .sslProvider(sslProvider)
                    .build();
            clientTransportFactory =
                address -> {
                  TcpClient client =
                      TcpClient.create().addressSupplier(() -> address).secure(sslContext);
                  return TcpClientTransport.create(client);
                };
          } catch (Exception sslException) {
            throw Exceptions.bubble(sslException);
          }
        }
      }

      this.accessTokenBytes = Base64.getDecoder().decode(accessToken);

      if (inetAddress == null) {
        try {
          inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          inetAddress = InetAddress.getLoopbackAddress();
        }
      }

      List<SocketAddress> socketAddresses;
      if (seedAddresses == null) {
        Objects.requireNonNull(host, "host is required");
        Objects.requireNonNull(port, "port is required");
        socketAddresses = Collections.singletonList(InetSocketAddress.createUnresolved(host, port));
      } else {
        socketAddresses = seedAddresses;
      }

      logger.info("registering with netifi with group {}", group);

      String proteusKey = accessKey + group;

      return PROTEUS.computeIfAbsent(
          proteusKey,
          _k -> {
            Proteus proteus =
                new Proteus(
                    accessKey,
                    Unpooled.wrappedBuffer(accessTokenBytes),
                    inetAddress,
                    group,
                    tags,
                    keepalive,
                    tickPeriodSeconds,
                    ackTimeoutSeconds,
                    missedAcks,
                    socketAddresses,
                    addressSelector,
                    clientTransportFactory,
                    poolSize,
                    tracerSupplier);
            proteus.onClose.doFinally(s -> PROTEUS.remove(proteusKey)).subscribe();

            return proteus;
          });
    }
  }
}
