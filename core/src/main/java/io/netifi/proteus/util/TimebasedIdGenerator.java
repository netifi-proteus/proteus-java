package io.netifi.proteus.util;

import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.LongSupplier;

public class TimebasedIdGenerator implements LongSupplier {
  private static final AtomicIntegerFieldUpdater<TimebasedIdGenerator> COUNTER =
      AtomicIntegerFieldUpdater.newUpdater(TimebasedIdGenerator.class, "counter");
  private final int id;

  @SuppressWarnings("unused")
  private volatile int counter = 0;

  public TimebasedIdGenerator(int id) {
    this.id = Math.abs(id) << 16;

    LocalTime now = LocalTime.now(ZoneOffset.UTC);
    counter = now.getHour();
  }

  public long nextId() {
    int count = COUNTER.getAndIncrement(this) & 65_535;
    long time = System.currentTimeMillis() << 32;
    return time | count | id;
  }

  @Override
  public long getAsLong() {
    return nextId();
  }
}
