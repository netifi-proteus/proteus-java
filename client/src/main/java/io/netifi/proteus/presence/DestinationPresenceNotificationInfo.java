package io.netifi.proteus.presence;

public interface DestinationPresenceNotificationInfo {
  String getDestination();

  long getAccountId();

  String getGroup();
}
