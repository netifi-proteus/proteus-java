package io.netifi.proteus.admin.tracing.internal;

import java.util.List;

public class Connections {
  private List<Connection> connections;

  private List<String> metadata;

  private String clazz;

  public List<Connection> getConnections() {
    return connections;
  }

  public void setConnections(List<Connection> connections) {
    this.connections = connections;
  }

  public List<String> getMetadata() {
    return metadata;
  }

  public void setMetadata(List<String> metadata) {
    this.metadata = metadata;
  }

  public String getClazz() {
    return clazz;
  }

  public void setClazz(String clazz) {
    this.clazz = clazz;
  }
}
