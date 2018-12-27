package io.netifi.proteus.common.time;

import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface Clock {
  long getEpochTime();

  default long elapsedSince(long timestamp) {
    long t = getEpochTime();
    return Math.max(0L, t - timestamp);
  }

  default TimeUnit unit() {
    return TimeUnit.MILLISECONDS;
  }

  Clock DEFAULT = System::currentTimeMillis;
}
