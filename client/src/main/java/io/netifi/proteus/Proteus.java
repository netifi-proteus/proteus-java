package io.netifi.proteus;

import io.micrometer.core.instrument.MeterRegistry;
import io.netifi.proteus.metrics.ProteusMetricsExporter;
import io.netifi.proteus.metrics.ProteusOperatingSystemMetrics;
import io.netifi.proteus.rs.ProteusSocket;
import io.netifi.proteus.rs.RequestHandlingRSocket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.proteus.metrics.om.MetricsSnapshotHandler;
import io.proteus.metrics.om.MetricsSnapshotHandlerClient;
import io.rsocket.Closeable;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** This is where the magic happens */
public class Proteus implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(Proteus.class);
  private static final ConcurrentHashMap<String, Proteus> PROTEUS = new ConcurrentHashMap<>();

  static {
    // Set the Java DNS cache to 60 seconds
    java.security.Security.setProperty("networkaddress.cache.ttl", "60");
  }

  private final String fromGroup;
  private final ProteusBrokerService brokerService;
  private volatile boolean running = true;
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
      Function<SocketAddress, ClientTransport> clientTransportFactory) {
    this.onClose = MonoProcessor.create();
    this.fromGroup = group;
    this.requestHandlingRSocket = new RequestHandlingRSocket();
    this.brokerService =
        new DefaultProteusBrokerService(
            seedAddresses,
            requestHandlingRSocket,
            fromGroup,
            destinationNameFactory,
            clientTransportFactory,
            keepalive,
            tickPeriodSeconds,
            ackTimeoutSeconds,
            missedAcks,
            accessKey,
            accessToken);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void dispose() {
    running = false;
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

  public Proteus addService(ProteusService service) {
    requestHandlingRSocket.addService(service);
    return this;
  }

  public ProteusSocket destination(String destination, String group) {
    return brokerService.destination(destination, group);
  }

  public ProteusSocket group(String group) {
    return brokerService.group(group);
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
    private boolean keepalive = DefaultBuilderConfig.getKeepAlive();
    private long tickPeriodSeconds = DefaultBuilderConfig.getTickPeriodSeconds();
    private long ackTimeoutSeconds = DefaultBuilderConfig.getAckTimeoutSeconds();
    private int missedAcks = DefaultBuilderConfig.getMissedAcks();
    private DestinationNameFactory destinationNameFactory;

    private MeterRegistry registry = null;
    private String metricHandlerGroup = DefaultBuilderConfig.getMetricHandlerGroup();
    private int batchSize = DefaultBuilderConfig.getBatchSize();
    private long exportFrequencySeconds = DefaultBuilderConfig.getExportFrequencySeconds();
    private boolean exportSystemMetrics = DefaultBuilderConfig.getExportSystemMetrics();
    private Function<SocketAddress, ClientTransport> clientTransportFactory =
        address -> TcpClientTransport.create((InetSocketAddress) address);

    private Builder clientTransportFactorty(
        Function<SocketAddress, ClientTransport> clientTransportFactory) {
      this.clientTransportFactory = clientTransportFactory;
      return this;
    }

    public Builder meterRegistry(MeterRegistry registry) {
      this.registry = registry;
      return this;
    }

    public Builder metricHandlerGroup(String metricHandlerGroup) {
      this.metricHandlerGroup = metricHandlerGroup;
      return this;
    }

    public Builder metricBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder metricExportFrequencySeconds(long exportFrequencySeconds) {
      this.exportFrequencySeconds = exportFrequencySeconds;
      return this;
    }

    public Builder exportSystemMetrics(boolean exportSystemMetrics) {
      this.exportSystemMetrics = exportSystemMetrics;
      return this;
    }

    public Builder keepalive(boolean useKeepAlive) {
      this.keepalive = keepalive;
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

    public Builder seedAddresses(Collection<SocketAddress> addresses) {
      if (addresses instanceof List) {
        this.seedAddresses = (List<SocketAddress>) addresses;
      } else {
        List<SocketAddress> list = new ArrayList<>();
        list.addAll(addresses);
        this.seedAddresses = list;
      }

      return this;
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
        socketAddresses = Arrays.asList(InetSocketAddress.createUnresolved(host, port));
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
                    clientTransportFactory);
            proteus.onClose.doFinally(s -> PROTEUS.remove(proteusKey)).subscribe();

            if (registry != null) {
              registry
                  .config()
                  .commonTags(
                      "accessKey",
                      String.valueOf(accessKey),
                      "group",
                      group,
                      "destination",
                      destination);

              ProteusOperatingSystemMetrics systemMetrics =
                  new ProteusOperatingSystemMetrics(registry);

              ProteusSocket socket = proteus.group(metricHandlerGroup);
              MetricsSnapshotHandler handler = new MetricsSnapshotHandlerClient(socket);
              ProteusMetricsExporter exporter =
                  new ProteusMetricsExporter(
                      null, registry, Duration.ofSeconds(exportFrequencySeconds), batchSize);
              exporter.run();
              proteus
                  .onClose
                  .doFinally(
                      s -> {
                        exporter.dispose();
                      })
                  .subscribe();
            }

            return proteus;
          });
    }
  }
}
