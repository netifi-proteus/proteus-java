package io.netifi.proteus.metrics;


import io.micrometer.core.instrument.AbstractMeter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.util.MeterEquivalence;

public class SpectatorCounter extends AbstractMeter implements Counter {
    private com.netflix.spectator.api.Counter counter;
    
    SpectatorCounter(Meter.Id id, com.netflix.spectator.api.Counter counter) {
        super(id);
        this.counter = counter;
    }
    
    @Override
    public void increment(double amount) {
        counter.increment((long) amount);
    }
    
    @Override
    public double count() {
        return counter.count();
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
