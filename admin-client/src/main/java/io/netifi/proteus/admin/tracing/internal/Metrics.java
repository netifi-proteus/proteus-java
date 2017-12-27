package io.netifi.proteus.admin.tracing.internal;

public class Metrics {
  private DecayingCounter normalCounter;
  private DecayingCounter dangerCounter;
  private DecayingCounter warningCounter;
  private Ewma normalAvg;
  private Ewma dangerAvg;
  private Ewma warningAvg;

  public Metrics() {
    normalAvg = new Ewma();
    dangerAvg = new Ewma();
    warningAvg = new Ewma();

    normalCounter = new DecayingCounter(1.0, 0);
    dangerCounter = new DecayingCounter(1.0, 0);
    warningCounter = new DecayingCounter(1.0, 0);
  }

  public Double getNormal() {
    return normalAvg.value();
  }

  public void setNormal(Double normal) {
    if (normal != null) {
      normalCounter.increment(normal);
      normalAvg.insert(normalCounter.get());
    }
  }

  public Double getDanger() {
    return dangerAvg.value();
  }

  public void setDanger(Double danger) {
    if (danger != null) {
      dangerCounter.increment(danger);
      dangerAvg.insert(dangerCounter.get());
    }
  }

  public Double getWarning() {
    return warningAvg.value();
  }

  public void setWarning(Double warning) {
    if (warning != null) {
      warningCounter.increment(warning);
      warningAvg.insert(warningCounter.get());
    }
  }
}
