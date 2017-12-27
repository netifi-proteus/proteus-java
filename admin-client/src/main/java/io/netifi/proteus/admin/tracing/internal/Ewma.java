package io.netifi.proteus.admin.tracing.internal;

public class Ewma {
  private double decay = 2.0 / (30_000 + 1.0);

  private volatile double avg;

  public synchronized void insert(double value) {
    avg = (value * decay) + (avg * (1 - decay));
  }

  public double value() {
    return avg;
  }
}
