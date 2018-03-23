package io.netifi.proteus.metrics;

import com.netflix.spectator.api.Measurement;
import com.netflix.spectator.api.Statistic;
import io.micrometer.core.instrument.AbstractDistributionSummary;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.util.MeterEquivalence;
import io.micrometer.core.lang.Nullable;

import static java.util.stream.StreamSupport.stream;

public class SpectatorDistributionSummary extends AbstractDistributionSummary {
  private final com.netflix.spectator.api.DistributionSummary summary;

  SpectatorDistributionSummary(
      Meter.Id id,
      com.netflix.spectator.api.DistributionSummary distributionSummary,
      Clock clock,
      DistributionStatisticConfig distributionStatisticConfig,
      double scale) {
    super(id, clock, distributionStatisticConfig, scale);
    this.summary = distributionSummary;
  }

  /**
   * @param amount Amount for an event being measured. For this implementation, amount is truncated
   *     to a long because the underlying Spectator implementation takes a long.
   */
  @Override
  protected void recordNonNegative(double amount) {
    summary.record((long) amount);
  }

  @Override
  public long count() {
    return summary.count();
  }

  @Override
  public double totalAmount() {
    return summary.totalAmount();
  }

  @Override
  public double max() {
    for (Measurement measurement : summary.measure()) {
      if (stream(measurement.id().tags().spliterator(), false)
          .anyMatch(
              tag ->
                  tag.key().equals("statistic") && tag.value().equals(Statistic.max.toString()))) {
        return measurement.value();
      }
    }

    return Double.NaN;
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(@Nullable Object o) {
    return MeterEquivalence.equals(this, o);
  }

  @Override
  public int hashCode() {
    return MeterEquivalence.hashCode(this);
  }
}
