package io.netifi.proteus.admin.tracing.internal;

import java.util.List;

public class Node {
  private String renderer;
  private String name;
  private String entryNode;
  private Double maxVolume;
  private String clazz;

  private Long updated;
  private List<Node> nodes;
  private List<Connection> connections;
  private String displayName;
  private List<String> metadata;

  private Metrics metrics;

  public String getRenderer() {
    return renderer;
  }

  public void setRenderer(String renderer) {
    this.renderer = renderer;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Double getMaxVolume() {
    return maxVolume;
  }

  public void setMaxVolume(Double maxVolume) {
    this.maxVolume = maxVolume;
  }

  public String getClazz() {
    return clazz;
  }

  public void setClazz(String clazz) {
    this.clazz = clazz;
  }

  public Long getUpdated() {
    return updated;
  }

  public void setUpdated(Long updated) {
    this.updated = updated;
  }

  public List<Node> getNodes() {
    return nodes;
  }

  public void setNodes(List<Node> nodes) {
    this.nodes = nodes;
  }

  public List<Connection> getConnections() {
    return connections;
  }

  public void setConnections(List<Connection> connections) {
    this.connections = connections;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public List<String> getMetadata() {
    return metadata;
  }

  public void setMetadata(List<String> metadata) {
    this.metadata = metadata;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public String getEntryNode() {
    return entryNode;
  }

  public void setEntryNode(String entryNode) {
    this.entryNode = entryNode;
  }
}
