package io.netifi.proteus;

import io.netifi.proteus.rsocket.NamedRSocketClientWrapper;
import io.netifi.proteus.rsocket.NamedRSocketServiceWrapper;
import io.netifi.proteus.rsocket.ProteusSocket;
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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final String fromGroup;
  private final DestinationNameFactory destinationNameFactory;
  private final ProteusBrokerService brokerService;
  private MonoProcessor<Void> onClose;
  private RequestHandlingRSocket requestHandlingRSocket;

  private Proteus(
      long accessKey,
      String group,
      ByteBuf accessToken,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      DestinationNameFactory destinationNameFactory,
      List<SocketAddress> seedAddresses,
      Function<SocketAddress, ClientTransport> clientTransportFactory,
      int poolSize,
      Supplier<Tracer> tracerSupplier) {
    this.accesskey = accessKey;
    this.onClose = MonoProcessor.create();
    this.fromGroup = group;
    this.requestHandlingRSocket = new RequestHandlingRSocket();
    this.destinationNameFactory = destinationNameFactory;
    this.brokerService =
        new DefaultProteusBrokerService(
            seedAddresses,
            requestHandlingRSocket,
            fromGroup,
            destinationNameFactory,
            clientTransportFactory,
            poolSize,
            keepalive,
            tickPeriodSeconds,
            ackTimeoutSeconds,
            missedAcks,
            accessKey,
            accessToken,
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
    Objects.requireNonNull(destination);
    Objects.requireNonNull(group);
    return brokerService.destination(destination, group);
  }

  @Deprecated
  public ProteusSocket group(String group) {
    Objects.requireNonNull(group);
    return brokerService.group(group);
  }

  @Deprecated
  public ProteusSocket broadcast(String group) {
    Objects.requireNonNull(group);
    return brokerService.broadcast(group);
  }

  public ProteusSocket destinationServiceSocket(String destination, String group) {
    return destination(destination, group);
  }

  public ProteusSocket groupServiceSocket(String group) {
    return group(group);
  }

  public ProteusSocket broadcastServiceSocket(String group) {
    return broadcast(group);
  }

  public ProteusSocket destinationNamedRSocket(String name, String destination, String group) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), destinationServiceSocket(destination, group));
  }

  public ProteusSocket groupNamedRSocket(String name, String group) {
    return NamedRSocketClientWrapper.wrap(Objects.requireNonNull(name), groupServiceSocket(group));
  }

  public ProteusSocket broadcastNamedRSocket(String name, String group) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), broadcastServiceSocket(group));
  }

  public long getAccesskey() {
    return accesskey;
  }

  public String getGroupName() {
    return fromGroup;
  }

  public String getDestination() {
    return destinationNameFactory.rootName();
  }

  public static class Builder {
    private String host = DefaultBuilderConfig.getHost();
    private Integer port = DefaultBuilderConfig.getPort();
    private List<SocketAddress> seedAddresses = DefaultBuilderConfig.getSeedAddress();
    private Long accessKey = DefaultBuilderConfig.getAccessKey();
    private String group = DefaultBuilderConfig.getGroup();
    private String destination = DefaultBuilderConfig.getDestination();
    private String accessToken = DefaultBuilderConfig.getAccessToken();
    private byte[] accessTokenBytes = new byte[20];
    private boolean sslDisabled = DefaultBuilderConfig.isSslDisabled();
    private boolean keepalive = DefaultBuilderConfig.getKeepAlive();
    private long tickPeriodSeconds = DefaultBuilderConfig.getTickPeriodSeconds();
    private long ackTimeoutSeconds = DefaultBuilderConfig.getAckTimeoutSeconds();
    private int missedAcks = DefaultBuilderConfig.getMissedAcks();
    private DestinationNameFactory destinationNameFactory;

    private Function<SocketAddress, ClientTransport> clientTransportFactory = null;
    private int poolSize = Runtime.getRuntime().availableProcessors();
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
     * @param address
     * @param addresses
     * @return
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

    public Builder destinationNameFactory(DestinationNameFactory destinationNameFactory) {
      this.destinationNameFactory = destinationNameFactory;
      return this;
    }

    public Proteus build() {
      Objects.requireNonNull(accessKey, "account key is required");
      Objects.requireNonNull(accessToken, "account token is required");
      Objects.requireNonNull(group, "group is required");

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

      if (destinationNameFactory == null) {
        if (destination == null) {
          destination = UUID.randomUUID().toString();
        }

        destinationNameFactory = DestinationNameFactory.from(destination, new AtomicInteger());
      }

      List<SocketAddress> socketAddresses;
      if (seedAddresses == null) {
        Objects.requireNonNull(host, "host is required");
        Objects.requireNonNull(port, "port is required");
        socketAddresses = Collections.singletonList(InetSocketAddress.createUnresolved(host, port));
      } else {
        socketAddresses = seedAddresses;
      }

      logger.info("registering with netifi with group {}, and destination {}", group, destination);

      String proteusKey = accessKey + group + destination;

      return PROTEUS.computeIfAbsent(
          proteusKey,
          _k -> {
            Proteus proteus =
                new Proteus(
                    accessKey,
                    group,
                    Unpooled.wrappedBuffer(accessTokenBytes),
                    keepalive,
                    tickPeriodSeconds,
                    ackTimeoutSeconds,
                    missedAcks,
                    destinationNameFactory,
                    socketAddresses,
                    clientTransportFactory,
                    poolSize,
                    tracerSupplier);
            proteus.onClose.doFinally(s -> PROTEUS.remove(proteusKey)).subscribe();

            return proteus;
          });
    }
  }
}
