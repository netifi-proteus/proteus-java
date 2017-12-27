package io.netifi.proteus.admin.tracing.internal;

public class DecayingCounter {

  private final double tau;
  private volatile double value;
  private long time;

  /**
   * Creates a new decaying counter with an initial value of 0.
   *
   * @param halfLife half-life in seconds.
   */
  public DecayingCounter(double halfLife) {
    this(halfLife, 0.0);
  }

  /**
   * Creates a new decaying counter.
   *
   * @param halfLife half-life in seconds.
   * @param initialValue initial value of the counter.
   */
  public DecayingCounter(double halfLife, double initialValue) {
    tau = halfLife / Math.log(2.0);
    set(initialValue);
  }

  /** Sets the counter to a new value. */
  public synchronized void set(double newValue) {
    value = newValue;
    time = System.nanoTime();
  }

  /** Returns the current counter value. */
  public double get() {
    update();
    return value;
  }

  /** Increments the counter by 1 and returns the new value. */
  public synchronized double increment() {
    update();
    value += 1.0;
    return value;
  }

  /** Increments the counter by 1 and returns the new value. */
  public synchronized double increment(double incr) {
    update();
    value += incr;
    return value;
  }

  private synchronized void update() {
    long newTime = System.nanoTime();
    long deltaTime = newTime - time;
    if (deltaTime > 0) value *= Math.exp(deltaTime * -1E-9 / tau);
    time = newTime;
  }
}
