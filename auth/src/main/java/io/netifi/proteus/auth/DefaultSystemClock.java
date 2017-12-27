package io.netifi.proteus.auth;

/** */
public class DefaultSystemClock implements Clock {
  @Override
  public long getEpochTime() {
    return System.currentTimeMillis();
  }
}
