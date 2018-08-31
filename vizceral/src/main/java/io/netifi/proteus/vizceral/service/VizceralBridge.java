package io.netifi.proteus.vizceral.service;

import com.google.protobuf.Empty;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.tracing.ProteusTraceStreamsSupplier;
import io.netifi.proteus.tracing.Trace;
import io.netifi.proteus.viz.Connection;
import io.netifi.proteus.viz.Metrics;
import io.netifi.proteus.viz.Node;
import io.netifi.proteus.viz.VizceralService;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import zipkin2.proto3.Annotation;
import zipkin2.proto3.Span;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VizceralBridge implements VizceralService {

  private final Flux<Trace> traceStreams;

  public VizceralBridge(Proteus proteus, Optional<String> tracingGroup) {
    this(new ProteusTraceStreamsSupplier(proteus, tracingGroup).get());
  }

  VizceralBridge(Flux<Trace> traceStreams) {
    this.traceStreams = traceStreams;
  }

  @Override
  public Flux<Node> visualisations(Empty message, ByteBuf metadata) {
    return Flux.defer(() -> {
      Flux<GroupedFlux<Edge, Edge>> edgesPerDestSvc = traceStreams
          .flatMap(this::edges)
          .groupBy(Function.identity());

      Map<String, Vertex> vertices = new ConcurrentHashMap<>();

      Flux<Connection> connections =
          edgesPerDestSvc
              .flatMap(edgesPerSvc -> {

                Edge edge = edgesPerSvc.key();

                String sourceNodeName = vizceralSourceName(edge);
                String targetNodeName = vizceralTargetName(edge);

                Flux<GroupedFlux<Boolean, Edge>> edgesBySuccess =
                    edgesPerSvc
                        .groupBy(e -> e.kind == Edge.Kind.SUCCESS)
                        .publish()
                        .autoConnect(2);

                Mono<Long> successCount = count(edgesBySuccess.filter(e -> e.key()));
                Mono<Long> errorCount = count(edgesBySuccess.filter(e -> !e.key()));

                Mono<Metrics> metrics = successCount
                    .zipWith(errorCount, (succ, err) -> {

                      long total = succ + err;
                      double errRate = err / (double) total;
                      double succRate = succ / (double) total;

                      long timeStamp = edge.getTimeStamp();

                      Vertex source = vertexByName(sourceNodeName, vertices);
                      source.updatedAt(timeStamp);

                      Vertex target = vertexByName(targetNodeName, vertices);
                      target.addMaxVolume(total);
                      target.updatedAt(timeStamp);

                      return Metrics.newBuilder()
                          .setNormal(succRate)
                          .setDanger(errRate)
                          .build();
                    });

                Mono<Connection> connection = metrics
                    .map(m -> Connection.newBuilder()
                        .setSource(sourceNodeName)
                        .setTarget(targetNodeName)
                        .setMetrics(m)
                        .build());

                return connection;
              });

      Mono<List<Connection>> connectionsCollection = connections.collectList();

      Mono<Node> rootNode = connectionsCollection
          .map(conns -> {

            List<Node> nodesCollection = vertices
                .values()
                .stream()
                .map(Vertex::toNode)
                .collect(Collectors.toList());

            return Node.newBuilder()
                .setRenderer("region")
                .setName("edge")
                .setEntryNode(entryNode(nodesCollection))
                .addAllConnections(conns)
                .addAllNodes(nodesCollection).build();
          });
      return rootNode;
    });
  }

  private Mono<Long> count(Flux<GroupedFlux<Boolean, Edge>> f) {
    return f
        .next()
        .flatMap(Flux::count)
        .switchIfEmpty(Mono.just(0L));
  }

  private String entryNode(List<Node> nodesCollection) {
    return nodesCollection.iterator().next().getName();
  }

  private Vertex vertexByName(String name, Map<String, Vertex> map) {
    return map.computeIfAbsent(name, this::createVertex);
  }

  private Vertex createVertex(String name) {
    return new Vertex(name);
  }

  private String vizceralSourceName(Edge edge) {
    return edge.getSourceGroup() + "-" + edge.getSourceDest() + "-" + edge.getSvc();
  }

  private String vizceralTargetName(Edge edge) {
    return edge.getTargetGroup() + "-" + edge.getTargetDest() + "-" + edge.getSvc();
  }

  private Flux<Edge> edges(Trace trace) {
    List<Span> spans = trace.getSpansList();
    if (spans.size() <= 1) {
      return Flux.empty();
    } else {
      return Flux.create(sink -> {
        for (int i = 1; i < spans.size(); i++) {
          Span target = spans.get(i);
          Map<String, String> targetTags = target.getTags();
          String side = targetTags.get("proteus.type");
          if ("server".equals(side)) {
            Edge.Kind kind = null;
            long timeStamp = -1;
            List<Annotation> annos = target.getAnnotationsList();
            for (Annotation annotation : annos) {
              String value = annotation.getValue();
              if ("onComplete".equals(value)) {
                kind = Edge.Kind.SUCCESS;
                timeStamp = annotation.getTimestamp();
                break;
              } else if ("onError".equals(value)) {
                kind = Edge.Kind.ERROR;
                timeStamp = annotation.getTimestamp();
                break;
              }
            }
            if (kind != null) {
              Span source = spans.get(i - 1);
              Map<String, String> sourceTargs = source.getTags();
              String sourceGroup = group(sourceTargs);
              String sourceDest = dest(sourceTargs);
              String svc = svc(sourceTargs);

              String targetGroup = group(targetTags);
              String targetDest = dest(targetTags);

              Edge edge = new Edge(
                  sourceGroup, sourceDest,
                  targetGroup, targetDest,
                  svc,
                  kind,
                  timeStamp);

              sink.next(edge);
            }
          }
        }
        sink.complete();
      });
    }
  }

  private String group(Map<String, String> tags) {
    return tags.get("group");
  }

  private String dest(Map<String, String> tags) {
    return tags.get("destination");
  }

  private String svc(Map<String, String> tags) {
    return tags.get("proteus.service");
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
      if (millis > updated) {
        updated = millis;
      }
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

    public Edge(String sourceGroup,
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
      SUCCESS, ERROR
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Edge edge = (Edge) o;
      return Objects.equals(sourceGroup, edge.sourceGroup) &&
          Objects.equals(sourceDest, edge.sourceDest) &&
          Objects.equals(targetGroup, edge.targetGroup) &&
          Objects.equals(targetDest, edge.targetDest) &&
          Objects.equals(svc, edge.svc);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceGroup, sourceDest, targetGroup, targetDest, svc);
    }
  }
}

