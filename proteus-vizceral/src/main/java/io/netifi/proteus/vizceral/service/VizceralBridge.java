package io.netifi.proteus.vizceral.service;

import io.netifi.proteus.tracing.ProteusTraceStreamsSupplier;
import io.netifi.proteus.tracing.Trace;
import io.netifi.proteus.tracing.TracesRequest;
import io.netifi.proteus.vizceral.*;
import io.netty.buffer.ByteBuf;
import io.rsocket.RSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class VizceralBridge implements VizceralService {
  private static final Logger logger = LoggerFactory.getLogger(VizceralBridge.class);

  private final Function<TracesRequest, Flux<Trace>> traceStreams;
  private int repeatIntervalSeconds;
  private int tracesLookbackSeconds;

  public VizceralBridge(
      Function<String, RSocket> rSocketFactory,
      Optional<String> tracingGroup,
      int intervalSeconds,
      int tracesLookbackSeconds) {
    this(
        new ProteusTraceStreamsSupplier(rSocketFactory, tracingGroup),
        intervalSeconds,
        tracesLookbackSeconds);
  }

  public VizceralBridge(Function<String, RSocket> rSocketFactory, Optional<String> tracingGroup) {
    this(rSocketFactory, tracingGroup, 5, 10);
  }

  public VizceralBridge(Function<String, RSocket> rSocketFactory) {
    this(rSocketFactory, Optional.empty(), 5, 10);
  }

  VizceralBridge(
      Function<TracesRequest, Flux<Trace>> traceStreams,
      int repeatIntervalSeconds,
      int tracesLookbackSeconds) {
    this.traceStreams = traceStreams;
    this.repeatIntervalSeconds = repeatIntervalSeconds;
    this.tracesLookbackSeconds = tracesLookbackSeconds;
  }

  @Override
  public Flux<Node> visualisations(com.google.protobuf.Empty empty, ByteBuf metadata) {

    return Flux.defer(
            () -> {
              Flux<Edge> groupedEdgesPerConnection =
                  traceStreams
                      .apply(traceRequest(tracesLookbackSeconds))
                      .onErrorResume(
                          err ->
                              Flux.error(
                                  new IllegalStateException(
                                      "Error reading traces source stream", err)))
                      .flatMap(this::buildEdges);
              // .groupBy(Function.identity());

              Map<String, Vertex> vertices = new ConcurrentHashMap<>();
              Map<Edge, EdgeState> edgeStates = new ConcurrentHashMap<>();
              RootNodeFinder rootNodeFinder = new RootNodeFinder();

              Flux<Connection> connections =
                  groupedEdgesPerConnection.map(
                      edge -> {
                        String sourceNodeName = vizceralSourceName(edge);
                        String targetNodeName = vizceralTargetName(edge);

                        rootNodeFinder.addEdge(sourceNodeName, targetNodeName);

                        EdgeState s = edgeStates.computeIfAbsent(edge, e -> new EdgeState());

                        if (edge.getKind() == Edge.Kind.SUCCESS) {
                          s.addSuccess();
                        } else {
                          s.addError();
                        }
                        s.addService(edge.getSvc());
                        s.updateTimeStamp(edge.getTimeStamp());
                        long timeStamp = s.getTimestamp();

                        Vertex source = vertexByName(sourceNodeName, vertices);
                        source.updatedAt(timeStamp);

                        Vertex target = vertexByName(targetNodeName, vertices);
                        target.updatedAt(timeStamp);
                        target.addMaxVolume(s.getTotal());

                        return Connection.newBuilder()
                            .setSource(sourceNodeName)
                            .setTarget(targetNodeName)
                            .setMetrics(s.metrics())
                            .addAllNotices(s.notices())
                            .setUpdated(s.getTimestamp())
                            .build();
                      });

              return connections.flatMap(
                  conn -> {
                    List<Node> nodesCollection =
                        vertices.values().stream().map(Vertex::toNode).collect(Collectors.toList());

                    String rootNodeName = rootNodeFinder.getRootNode();

                    Node root =
                        vertices
                            .get(rootNodeName)
                            .toNodeBuilder()
                            .setMaxVolume(0)
                            .setEntryNode(rootNodeName)
                            .setRenderer("region")
                            .addConnections(conn)
                            .addAllNodes(nodesCollection)
                            .build();

                    return Mono.just(root);
                  });
            })
        .onErrorResume(new BackOff())
        .retry()
        .repeatWhen(
            flux -> flux.flatMap(v -> Mono.delay(Duration.ofSeconds(repeatIntervalSeconds))));
  }

  private static TracesRequest traceRequest(int tracesLookbackSeconds) {
    return TracesRequest.newBuilder().setLookbackSeconds(tracesLookbackSeconds).build();
  }

  private Vertex vertexByName(String name, Map<String, Vertex> map) {
    return map.computeIfAbsent(name, this::createVertex);
  }

  private Vertex createVertex(String name) {
    return new Vertex(name);
  }

  private String vizceralSourceName(Edge edge) {
    return edge.getSourceGroup() + "-" + edge.getSourceDest();
  }

  private String vizceralTargetName(Edge edge) {
    return edge.getTargetGroup() + "-" + edge.getTargetDest();
  }

  private Flux<Edge> buildEdges(Trace trace) {
    return Flux.create(new EdgesBuilder(trace.getSpansList()));
  }

  static class RootNodeFinder {
    private final Set<String> responders = new HashSet<>();
    private final Set<String> requesters = new HashSet<>();

    public void addEdge(String source, String target) {
      responders.add(target);
      requesters.remove(target);
      if (!responders.contains(source)) {
        requesters.add(source);
      }
    }

    public String getRootNode() {
      Set<String> target =
          !requesters.isEmpty() ? requesters : !responders.isEmpty() ? responders : null;
      return Optional.ofNullable(target).map(v -> v.iterator().next()).orElse("");
    }
  }

  static class EdgeState {
    private int success;
    private int error;
    private Set<String> services = new HashSet<>();
    private long timestamp;

    public void addSuccess() {
      success++;
    }

    public void addError() {
      error++;
    }

    public void addService(String svc) {
      services.add(svc);
    }

    public void updateTimeStamp(long timestamp) {
      this.timestamp = Math.max(this.timestamp, timestamp);
    }

    public Metrics metrics() {
      return Metrics.newBuilder().setNormal(successRate()).setDanger(errorRate()).build();
    }

    public int getTotal() {
      return success + error;
    }

    public double successRate() {
      return success / (double) getTotal();
    }

    public double errorRate() {
      return error / (double) getTotal();
    }

    public Set<String> getServices() {
      return new HashSet<>(services);
    }

    public long getTimestamp() {
      return timestamp;
    }

    public Collection<Notice> notices() {
      return services
          .stream()
          .map(s -> Notice.newBuilder().setTitle(s).setSeverity(1).build())
          .collect(Collectors.toList());
    }
  }

  static class Vertex {
    /*written from single thread*/
    private volatile long maxVolume = 0;
    private final String name;
    /*written from single thread*/
    private volatile long updated;

    public Vertex(String name) {
      this.name = name;
    }

    public void updatedAt(long millis) {
      updated = Math.max(millis, updated);
    }

    public void addMaxVolume(long maxVolume) {
      this.maxVolume += maxVolume;
    }

    public Node toNode() {
      return toNodeBuilder().build();
    }

    public Node.Builder toNodeBuilder() {
      return Node.newBuilder()
          .setName(name)
          .setDisplayName(name)
          .setClass_("normal")
          .setMaxVolume(maxVolume)
          .setUpdated(updated);
    }
  }

  static class Edge {
    private final String sourceGroup;
    private final String sourceDest;
    private final String targetGroup;
    private final String targetDest;
    private final String svc;
    private final Kind kind;
    private final long timeStamp;

    public Edge(
        String sourceGroup,
        String sourceDest,
        String targetGroup,
        String targetDest,
        String svc,
        Kind kind,
        long timeStamp) {
      this.sourceGroup = sourceGroup;
      this.sourceDest = sourceDest;
      this.targetGroup = targetGroup;
      this.targetDest = targetDest;
      this.svc = svc;
      this.kind = kind;
      this.timeStamp = timeStamp;
    }

    public long getTimeStamp() {
      return timeStamp;
    }

    public String getSourceGroup() {
      return sourceGroup;
    }

    public String getSourceDest() {
      return sourceDest;
    }

    public String getTargetGroup() {
      return targetGroup;
    }

    public String getTargetDest() {
      return targetDest;
    }

    public String getSvc() {
      return svc;
    }

    public Kind getKind() {
      return kind;
    }

    enum Kind {
      SUCCESS,
      ERROR
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Edge edge = (Edge) o;
      return Objects.equals(sourceGroup, edge.sourceGroup)
          && Objects.equals(sourceDest, edge.sourceDest)
          && Objects.equals(targetGroup, edge.targetGroup)
          && Objects.equals(targetDest, edge.targetDest);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceGroup, sourceDest, targetGroup, targetDest);
    }
  }

  private static class BackOff implements Function<Throwable, Publisher<? extends Node>> {
    AtomicLong lastRetry = new AtomicLong(System.currentTimeMillis());
    AtomicInteger count = new AtomicInteger();

    @Override
    public Publisher<? extends Node> apply(Throwable throwable) {
      if (System.currentTimeMillis() - lastRetry.getAndSet(System.currentTimeMillis()) > 30_000) {
        count.set(0);
      }

      int i = Math.min(30, count.incrementAndGet());
      logger.error("error getting vizceral data", throwable);
      return Mono.delay(Duration.ofSeconds(i)).then(Mono.error(throwable));
    }
  }
}
