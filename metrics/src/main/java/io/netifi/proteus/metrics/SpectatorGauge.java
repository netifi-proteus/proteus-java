package io.netifi.proteus.metrics;

import io.micrometer.core.instrument.AbstractMeter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.util.MeterEquivalence;

public class SpectatorGauge extends AbstractMeter implements Gauge {
    private com.netflix.spectator.api.Gauge gauge;
    
    SpectatorGauge(Id id, com.netflix.spectator.api.Gauge gauge) {
        super(id);
        this.gauge = gauge;
    }
    
    @Override
    public double value() {
        return gauge.value();
    }
    
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return MeterEquivalence.equals(this, o);
    }
    
    @Override
    public int hashCode() {
        return MeterEquivalence.hashCode(this);
    }
}
