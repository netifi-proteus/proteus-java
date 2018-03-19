package io.netifi.proteus.admin.tracing;

import io.netifi.proteus.admin.connection.ConnectionManager;
import io.netifi.proteus.admin.om.Notice;
import io.netifi.proteus.admin.tracing.internal.Connection;
import io.netifi.proteus.admin.tracing.internal.Metrics;
import io.netifi.proteus.admin.tracing.internal.Node;
import io.netifi.proteus.frames.RouteDestinationFlyweight;
import io.netifi.proteus.frames.RouteType;
import io.netifi.proteus.frames.RoutingFlyweight;
import io.netifi.proteus.frames.admin.AdminFrameHeaderFlyweight;
import io.netifi.proteus.frames.admin.AdminFrameType;
import io.netifi.proteus.frames.admin.AdminTraceMetadataFlyweight;
import io.netifi.proteus.frames.admin.AdminTraceType;
import io.netifi.proteus.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultAdminTraceService implements AdminTraceService, Closeable {
  private static final Logger logger = LoggerFactory.getLogger(DefaultAdminTraceService.class);
  private final MonoProcessor<Void> onClose;
  private final Flux<io.netifi.proteus.admin.om.Node> streamData;

  public DefaultAdminTraceService(
      TimebasedIdGenerator idGenerator, ConnectionManager connectionManager) {
    this.onClose = MonoProcessor.create();

    streamData =
        connectionManager
            .getRSockets()
            .flatMap(
                rsocket -> {
                  logger.debug(
                      "streaming tracing data from router id {} from address {}",
                      rsocket.getRouterId(),
                      rsocket.getSocketAddress());
                  Payload request = createPayload();
                  return rsocket
                      .requestStream(request)
                      .subscribeOn(Schedulers.elastic());
                })
            .doOnError(t -> logger.error(t.getMessage(), t))
            .filter(p -> {
              ByteBuf metadata = p.sliceMetadata();
              
              String fromGroup = AdminTraceMetadataFlyweight.group(metadata);
              
              return !"routerToRouter".equals(fromGroup);
            })
            .scan(createRootNode(), this::handleResponse)
            .filter(node -> !node.getNodes().isEmpty())
            .map(
                node -> {
                  List<Node> nodes = node.getNodes();
                  Node node1 = nodes.get(0);
                  node.setEntryNode(node1.getName());
                  return node;
                })
            .sample(Duration.ofSeconds(1))
            .cache(1)
            .flatMap(node -> {
              return Flux.interval(Duration.ofSeconds(1)).map(l -> node);
            })
            .onBackpressureLatest()
            .map(this::translateNode)
            .publish()
            .refCount();
  }

  private Node handleResponse(Node rootNode, Payload response) {
    ByteBuf data = response.sliceData();
    ByteBuf metadata = response.sliceMetadata();

    AdminTraceType adminTraceType = AdminTraceMetadataFlyweight.adminTraceType(metadata);
    double metric = AdminTraceMetadataFlyweight.metric(metadata);
    String routerAddress = AdminTraceMetadataFlyweight.routerAddress(metadata);
    String fromGroup = AdminTraceMetadataFlyweight.group(metadata);

    String fromDestination = RoutingFlyweight.destination(data);
    long fromAccessKey = RoutingFlyweight.accessKey(data);

    ByteBuf route = RoutingFlyweight.route(data);
    long accountId = RouteDestinationFlyweight.accountId(route);
    String destination = RouteDestinationFlyweight.destination(route);
    String group = RouteDestinationFlyweight.group(route);
    RouteType routeType = RouteDestinationFlyweight.routeType(route);

    // Update Nodes
    boolean foundSourceNode =
        rootNode.getNodes().stream().anyMatch(n -> n.getName().equals(fromGroup));

    if (!foundSourceNode) {
      Node newNode = new Node();
      newNode.setName(fromGroup);
      newNode.setDisplayName(fromGroup);
      newNode.setClazz("normal");
      newNode.setMaxVolume(5000D);
      rootNode.getNodes().add(newNode);
    }

    boolean foundTargetNode = rootNode.getNodes().stream().anyMatch(n -> n.getName().equals(group));

    if (!foundTargetNode) {
      Node newNode = new Node();
      newNode.setName(group);
      newNode.setDisplayName(group);
      newNode.setClazz("normal");
      newNode.setMaxVolume(5000D);
      rootNode.getNodes().add(newNode);
    }

    // Update Connections

    Connection connection =
        rootNode
            .getConnections()
            .stream()
            .filter(c -> c.getSource().equals(fromGroup) && c.getTarget().equals(group))
            .findFirst()
            .orElseGet(
                () -> {
                  Connection c = new Connection();
                  c.setMetrics(new Metrics());
                  c.setClazz("normal");
                  c.setSource(fromGroup);
                  c.setTarget(group);
                  rootNode.getConnections().add(c);
                  return c;
                });

    switch (adminTraceType) {
      case ERROR_FIRE_FORGET:
      case ERROR_METADATA_PUSH:
      case ERROR_REQUEST_REPLY:
      case ERROR_REQUEST_STREAM:
      case ERROR_REQUEST_CHANNEL:
        connection.getMetrics().setDanger(1.0d);
      case CANCEL_FIRE_FORGET:
      case CANCEL_METADATA_PUSH:
      case CANCEL_REQUEST_REPLY:
      case COMPLETE_FIRE_FORGET:
      case CANCEL_REQUEST_STREAM:
      case CANCEL_REQUEST_CHANNEL:
        connection.getMetrics().setWarning(1.0d);
      case COMPLETE_METADATA_PUSH:
      case COMPLETE_REQUEST_REPLY:
      case ON_NEXT_REQUEST_CHANEL:
      case ON_NEXT_REQUEST_STREAM:
      case COMPLETE_REQUEST_STREAM:
      case COMPLETE_REQUEST_CHANNEL:
        connection.getMetrics().setNormal(1.0d);
      case UNDEFINED:
      default:
    }

    return rootNode;
  }

  private Payload createPayload() {
    int length = AdminFrameHeaderFlyweight.computeLength("test");
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(length);
    AdminFrameHeaderFlyweight.encode(
        byteBuf,
        AdminFrameType.ADMIN_FRAME_TRACE,
        System.currentTimeMillis(),
        Long.MAX_VALUE,
        "test");

    return ByteBufPayload.create(Unpooled.EMPTY_BUFFER, byteBuf);
  }

  private Node createRootNode() {
    Node node = new Node();
    node.setNodes(new ArrayList<>());
    node.setConnections(new ArrayList<>());
    node.setName("edge");
    node.setRenderer("region");
    node.setMaxVolume(5000D);
    return node;
  }

  @Override
  public Flux<io.netifi.proteus.admin.om.Node> streamData() {
    return streamData;
  }

  private io.netifi.proteus.admin.om.Node translateNode(Node node) {
    List<io.netifi.proteus.admin.om.Connection> connections =
        translateConnections(node.getConnections());

    List<io.netifi.proteus.admin.om.Node> nodes;
    if (node.getNodes() != null) {
      nodes = node.getNodes().stream().map(this::translateNode).collect(Collectors.toList());
    } else {
      nodes = Collections.EMPTY_LIST;
    }

    io.netifi.proteus.admin.om.Metrics metrics = translateMetric(node.getMetrics());

    return io.netifi.proteus.admin.om.Node.newBuilder()
        .setRenderer(node.getRenderer() == null ? "" : node.getRenderer())
        .setName(node.getName() == null ? "" : node.getName())
        .setEntryNode(node.getEntryNode() == null ? "" : node.getEntryNode())
        .setMaxVolume(node.getMaxVolume() == null ? 0.0 : node.getMaxVolume())
        .setClass_(node.getClazz() == null ? "" : node.getClazz())
        .setUpdated(node.getUpdated() == null ? 0 : node.getUpdated())
        .addAllNodes(nodes)
        .addAllConnections(connections)
        .setDisplayName(node.getDisplayName() == null ? "" : node.getDisplayName())
        .addAllMetadata(node.getMetadata() == null ? Collections.EMPTY_LIST : node.getMetadata())
        .setMetrics(metrics)
        .build();
  }

  private List<io.netifi.proteus.admin.om.Connection> translateConnections(
      List<Connection> connections) {
    if (connections != null) {
      return connections
          .stream()
          .map(
              connection -> {
                List<Notice> notices = translateNotices(connection.getNotices());
                io.netifi.proteus.admin.om.Metrics metrics =
                    translateMetric(connection.getMetrics());

                return io.netifi.proteus.admin.om.Connection.newBuilder()
                    .setSource(connection.getSource())
                    .setTarget(connection.getTarget())
                    .setMetrics(metrics)
                    .addAllNotices(notices)
                    .build();
              })
          .collect(Collectors.toList());
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  private io.netifi.proteus.admin.om.Metrics translateMetric(Metrics metrics) {
    if (metrics != null) {
      return io.netifi.proteus.admin.om.Metrics.newBuilder()
          .setDanger(metrics.getDanger())
          .setNormal(metrics.getNormal())
          .setWarning(metrics.getWarning())
          .build();
    } else {
      return io.netifi.proteus.admin.om.Metrics.newBuilder().build();
    }
  }

  private List<io.netifi.proteus.admin.om.Notice> translateNotices(
      List<io.netifi.proteus.admin.tracing.internal.Notice> notices) {
    if (notices != null) {
      return notices.stream().map(this::translateNotice).collect(Collectors.toList());
    } else {
      return Collections.EMPTY_LIST;
    }
  }

  private io.netifi.proteus.admin.om.Notice translateNotice(
      io.netifi.proteus.admin.tracing.internal.Notice notice) {
    return io.netifi.proteus.admin.om.Notice.newBuilder()
        .setLink(notice.getLink())
        .setTitle(notice.getTitle())
        .build();
  }

  @Override
  public void dispose() {
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
}
