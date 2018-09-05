package io.netifi.proteus.vizceral.service;

import reactor.core.publisher.FluxSink;
import zipkin2.proto3.Annotation;
import zipkin2.proto3.Span;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

class EdgesBuilder implements Consumer<FluxSink<VizceralBridge.Edge>> {
  private final List<Span> spans;

  EdgesBuilder(List<Span> spans) {
    this.spans = spans;
  }

  @Override
  public void accept(FluxSink<VizceralBridge.Edge> sink) {
    if (spans.size() <= 1) {
      emptyEdges(sink);
    } else {
      buildEdges(sink);
    }
  }

  private void emptyEdges(FluxSink<VizceralBridge.Edge> sink) {
    sink.complete();
  }

  private void buildEdges(FluxSink<VizceralBridge.Edge> sink) {
    Map<String, Span> spansLookup = lookupByParentId(spans);
    Span prev = null;
    Span cur = rootSpan(spans);

    while (cur != null) {
      Map<String, String> curTags = cur.getTagsMap();

      if (isServer(role(curTags))) {
        VizceralBridge.Edge.Kind kind = null;
        long timeStamp = -1;
        List<Annotation> annos = cur.getAnnotationsList();

        for (Annotation annotation : annos) {
          String value = annotation.getValue();

          if (isOnComplete(value)) {
            kind = VizceralBridge.Edge.Kind.SUCCESS;
            timeStamp = annotation.getTimestamp();
            break;
          } else if (isOnError(value)) {
            kind = VizceralBridge.Edge.Kind.ERROR;
            timeStamp = annotation.getTimestamp();
            break;
          }
        }
        if (kind != null) {
          Map<String, String> prevTags = prev.getTagsMap();
          String sourceGroup = group(prevTags);
          String sourceDest = dest(prevTags);
          String svc = svc(prevTags);

          String targetGroup = group(curTags);
          String targetDest = dest(curTags);

          VizceralBridge.Edge edge = new VizceralBridge.Edge(
              sourceGroup, sourceDest,
              targetGroup, targetDest,
              svc,
              kind,
              timeStamp);

          sink.next(edge);
        }
      }
      prev = cur;
      cur = nextSpan(cur, spansLookup);
    }
    sink.complete();
  }

  private boolean isServer(String role) {
    return "server".equals(role);
  }

  private boolean isOnComplete(String value) {
    return "onComplete".equals(value);
  }

  private boolean isOnError(String value) {
    return "onError".equals(value);
  }

  private String role(Map<String, String> tags) {
    return tags.get("rsocket.rpc.role");
  }

  private String group(Map<String, String> tags) {
    return tags.get("group");
  }

  private String dest(Map<String, String> tags) {
    return tags.get("destination");
  }

  private String svc(Map<String, String> tags) {
    return tags.get("rsocket.service");
  }

  private Span nextSpan(Span cur, Map<String, Span> lookup) {
    return lookup.get(cur.getId());
  }

  private Span rootSpan(List<Span> spans) {
    for (Span span : spans) {
      if (isEmpty(span.getParentId())) {
        return span;
      }
    }
    throw new IllegalStateException("Trace without root span");
  }

  private boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }

  private Map<String, Span> lookupByParentId(List<Span> spans) {
    Map<String, Span> lookup = new HashMap<>();
    for (Span span : spans) {
      String parentId = span.getParentId();
      if (parentId != null) {
        lookup.put(parentId, span);
      }
    }
    return lookup;
  }
}
