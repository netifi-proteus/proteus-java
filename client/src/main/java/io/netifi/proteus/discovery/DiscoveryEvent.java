package io.netifi.proteus.discovery;

import java.net.SocketAddress;

/** DiscoveryEvent encapsulating when a address is added or removed */
public class DiscoveryEvent {
  private String id;
  private SocketAddress socketAddress;
  private DiscoveryEventType type;

  public DiscoveryEvent(String id, SocketAddress socketAddress, DiscoveryEventType type) {
    this.id = id;
    this.socketAddress = socketAddress;
    this.type = type;
  }

  public static DiscoveryEvent add(String id, SocketAddress socketAddress) {
    DiscoveryEvent add = new DiscoveryEvent(id, socketAddress, DiscoveryEventType.Add);
    return add;
  }

  public static DiscoveryEvent add(SocketAddress socketAddress) {
    DiscoveryEvent add =
        new DiscoveryEvent(socketAddress.toString(), socketAddress, DiscoveryEventType.Add);
    return add;
  }

  public static DiscoveryEvent remove(String id, SocketAddress socketAddress) {
    DiscoveryEvent remove = new DiscoveryEvent(id, socketAddress, DiscoveryEventType.Remove);
    return remove;
  }

  public static DiscoveryEvent remove(SocketAddress socketAddress) {
    DiscoveryEvent remove =
        new DiscoveryEvent(socketAddress.toString(), socketAddress, DiscoveryEventType.Remove);
    return remove;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DiscoveryEvent that = (DiscoveryEvent) o;

    return socketAddress.equals(that.socketAddress);
  }

  @Override
  public int hashCode() {
    return socketAddress.hashCode();
  }

  @Override
  public String toString() {
    return "DiscoveryEvent{" + "id='" + id + '\'' + ", seedAddresses=" + socketAddress + '}';
  }

  public SocketAddress getAddress() {
    return socketAddress;
  }

  public String getId() {
    return id;
  }

  public DiscoveryEventType getType() {
    return type;
  }

  public enum DiscoveryEventType {
    Add,
    Remove
  }
}
