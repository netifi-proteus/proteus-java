package io.netifi.proteus.admin.tracing.internal;

import java.time.Duration;

public class Metrics {
  private SmoothlyDecayingRollingCounter normalCounter;
  private SmoothlyDecayingRollingCounter dangerCounter;
  private SmoothlyDecayingRollingCounter warningCounter;
  
  public Metrics() {
    normalCounter = new SmoothlyDecayingRollingCounter(Duration.ofSeconds(30), 5);
    dangerCounter = new SmoothlyDecayingRollingCounter(Duration.ofSeconds(30), 5);
    warningCounter = new SmoothlyDecayingRollingCounter(Duration.ofSeconds(30), 5);
  }

  public Double getNormal() {
    return (double) normalCounter.getSum();
  }

  public void setNormal(Double normal) {
    normalCounter.add(normal.longValue());
  }

  public Double getDanger() {
    return (double) dangerCounter.getSum();
  }

  public void setDanger(Double danger) {
    dangerCounter.add(danger.longValue());
  }

  public Double getWarning() {
    return (double) warningCounter.getSum();
  }

  public void setWarning(Double warning) {
    warningCounter.add(warning.longValue());
  }
}
