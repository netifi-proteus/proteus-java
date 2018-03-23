package io.netifi.proteus.metrics;

import com.netflix.spectator.api.*;
import io.micrometer.core.lang.Nullable;

import java.util.Collections;
import java.util.function.ToDoubleFunction;

/**
 * Gauge that is defined by executing a {@link ToDoubleFunction} on an object.
 * This is identical to com.netflix.spectator.api.ObjectGauge which is not accessible in Spectator.
 */
class SpectatorToDoubleGauge<T> extends AbstractMeter<T> implements Gauge {
    
    private final ToDoubleFunction<T> f;
    
    SpectatorToDoubleGauge(Clock clock, Id id, @Nullable T obj, ToDoubleFunction<T> f) {
        super(clock, id, obj);
        this.f = f;
    }
    
    @Override
    public Iterable<Measurement> measure() {
        return Collections.singleton(new Measurement(id, clock.wallTime(), value()));
    }
    
    @Override
    public double value() {
        final T obj = ref.get();
        return (obj == null) ? Double.NaN : f.applyAsDouble(obj);
    }
}