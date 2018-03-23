package io.netifi.proteus.metrics;

import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.lang.Nullable;

import static com.netflix.spectator.api.Statistic.*;

public class AtlasUtils {
    @Nullable
    static com.netflix.spectator.api.Statistic toSpectatorStatistic(Statistic stat) {
        switch (stat) {
            case COUNT:
                return count;
            case TOTAL_TIME:
                return totalTime;
            case TOTAL:
                return totalAmount;
            case VALUE:
                return gauge;
            case ACTIVE_TASKS:
                return activeTasks;
            case DURATION:
                return duration;
            case MAX:
                return max;
        }
        return null;
    }
}
