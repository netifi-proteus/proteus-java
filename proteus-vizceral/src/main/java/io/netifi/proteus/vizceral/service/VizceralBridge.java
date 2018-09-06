package io.netifi.proteus.vizceral.service;

import io.netifi.proteus.Proteus;
import io.netifi.proteus.tracing.ProteusTraceStreamsSupplier;
import io.netifi.proteus.tracing.Trace;
import io.netifi.proteus.tracing.TracesRequest;
import io.netifi.proteus.vizceral.*;
import io.netty.buffer.ByteBuf;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

public class VizceralBridge implements VizceralService {

  private final Function<TracesRequest, Flux<Trace>> traceStreams;

  public VizceralBridge(Proteus proteus, Optional<String> tracingGroup) {
    this(new ProteusTraceStreamsSupplier(proteus, tracingGroup));
  }

  VizceralBridge(Function<TracesRequest, Flux<Trace>> traceStreams) {
    this.traceStreams = traceStreams;
  }

  @Override
  public Flux<Node> visualisations(VisualisationRequest message, ByteBuf metadata) {
    return Flux.defer(
        () -> {
          Flux<GroupedFlux<Edge, Edge>> groupedEdgesPerConnection =
              traceStreams
                  .apply(tracesFor(message))
                  .onErrorResume(
                      err ->
                          Flux.error(
                              new IllegalStateException("Error reading traces source stream", err)))
                  .flatMap(this::buildEdges)
                  .groupBy(Function.identity());

          Map<String, Vertex> vertices = new ConcurrentHashMap<>();
          Flux<Connection> connections =
              groupedEdgesPerConnection.flatMap(
                  edges -> {
                    Edge edge = edges.key();

                    String sourceNodeName = vizceralSourceName(edge);
                    String targetNodeName = vizceralTargetName(edge);

                    Mono<EdgeState> reducedEdge =
                        edges
                            .scan(
                                new EdgeState(),
                                (s, e) -> {
                                  if (edge.kind == Edge.Kind.SUCCESS) {
                                    s.addSuccess();
                                  } else {
                                    s.addError();
                                  }
                                  s.addService(e.getSvc());
                                  s.updateTimeStamp(e.getTimeStamp());
                                  return s;
                                })
                            .last();

                    Mono<Connection> connection =
                        reducedEdge
                            .doOnSuccess(
                                s -> {
                                  long timeStamp = s.getTimestamp();

                                  Vertex source = vertexByName(sourceNodeName, vertices);
                                  source.updatedAt(timeStamp);

                                  Vertex target = vertexByName(targetNodeName, vertices);
                                  target.updatedAt(timeStamp);
                                  target.addMaxVolume(s.getTotal());
                                })
                            .map(
                                s ->
                                    Connection.newBuilder()
                                        .setSource(sourceNodeName)
                                        .setTarget(targetNodeName)
                                        .setMetrics(s.metrics())
                                        .addAllNotices(s.notices())
                                        .build());

                    return connection;
                  });

          return connections
              .collectList()
              .map(
                  conns -> {
                    List<Node> nodesCollection =
                        vertices.values().stream().map(Vertex::toNode).collect(Collectors.toList());

                    String rootNodeName = message.getRootNode();

                    return Node.newBuilder()
                        .setRenderer("region")
                        .setName(rootNodeName)
                        .setEntryNode(rootNodeName)
                        .addAllConnections(conns)
                        .addAllNodes(nodesCollection)
                        .build();
                  });
        });
  }

  private TracesRequest tracesFor(VisualisationRequest message) {
    return TracesRequest.newBuilder().setLookbackSeconds(message.getDataLookbackSeconds()).build();
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
          .map(s -> Notice.newBuilder().setTitle(s).build())
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
      return Node.newBuilder()
          .setName(name)
          .setDisplayName(name)
          .setClass_("normal")
          .setMaxVolume(maxVolume)
          .setUpdated(updated)
          .build();
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
}
