package io.netifi.proteus.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.rsocket.util.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

public class ProteusOperatingSystemMetrics implements Disposable {
    private Logger logger = LoggerFactory.getLogger(ProteusOperatingSystemMetrics.class);
    private MeterRegistry registry;
    
    private Disposable disposable;
    
    public ProteusOperatingSystemMetrics(MeterRegistry registry) {
        this.registry = registry;
    
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    
        registry.gauge("system", Arrays.asList(Tag.of("stat", "processCpuLoad")), this, t -> getProcessCpuLoad());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "systemCpuLoad")), this, t -> getSystemCpuLoad());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "systemLoadAverage")), operatingSystemMXBean, OperatingSystemMXBean::getSystemLoadAverage);
        registry.gauge("system", Arrays.asList(Tag.of("stat", "availableProcessors")), operatingSystemMXBean, OperatingSystemMXBean::getAvailableProcessors);
        
        registry.gauge("system", Arrays.asList(Tag.of("stat", "heapMemoryUsage.commit")), memoryMXBean, value -> value.getHeapMemoryUsage().getCommitted());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "heapMemoryUsage.init")), memoryMXBean, value -> value.getHeapMemoryUsage().getInit());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "heapMemoryUsage.max")), memoryMXBean, value -> value.getHeapMemoryUsage().getMax());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "heapMemoryUsage.used")), memoryMXBean, value -> value.getHeapMemoryUsage().getUsed());
        
        registry.gauge("system", Arrays.asList(Tag.of("stat", "nonHeapMemoryUsage.commit")), memoryMXBean, value -> value.getNonHeapMemoryUsage().getCommitted());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "nonHeapMemoryUsage.init")), memoryMXBean, value -> value.getNonHeapMemoryUsage().getInit());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "nonHeapMemoryUsage.max")), memoryMXBean, value -> value.getNonHeapMemoryUsage().getMax());
        registry.gauge("system", Arrays.asList(Tag.of("stat", "nonHeapMemoryUsage.used")), memoryMXBean, value -> value.getNonHeapMemoryUsage().getUsed());
        
        disposable = Schedulers.single().schedulePeriodically(new Runnable() {
            NonBlockingHashMap<String, Long> countCallbacks = new NonBlockingHashMap<>();
            NonBlockingHashMap<String, Long> timeCallbacks = new NonBlockingHashMap<>();
            
            @Override
            public void run() {
                try {
                    Iterator<GarbageCollectorMXBean> iterator = garbageCollectorMXBeans.iterator();
                    while (iterator.hasNext()) {
                        GarbageCollectorMXBean garbageCollectorMXBean = iterator.next();
                        String name = garbageCollectorMXBean.getName();
            
                        countCallbacks.compute(name, (s,l) -> {
                            if (l == null) {
                                registry.gauge("system", Arrays.asList(Tag.of("stat", name + ".count")), this, t -> countCallbacks.get(name));
                            }
                            
                            return garbageCollectorMXBean.getCollectionCount();
                        });
    
                        timeCallbacks.compute(name, (s,l) -> {
                            if (l == null) {
                                registry.gauge("system", Arrays.asList(Tag.of("stat", name + ".time")), this, t -> countCallbacks.get(name));
                            }
        
                            return garbageCollectorMXBean.getCollectionTime();
                        });
                    }
        
                } catch (Throwable t) {
                    logger.debug(t.getMessage(), t);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    private static double getProcessCpuLoad() {
    
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});
        
            mbs.getMBeanInfo(name).getAttributes();
        
            if (list.isEmpty()) return Double.NaN;
        
            Attribute att = (Attribute) list.get(0);
            Double value = (Double) att.getValue();
            if (value == -1.0) return 0;
            return value;
        } catch (Exception e) {
            return 0;
        }
    }
    
    
    private static double getSystemCpuLoad() {
        
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[]{"SystemCpuLoad"});
            
            mbs.getMBeanInfo(name).getAttributes();
            
            if (list.isEmpty()) return Double.NaN;
            
            Attribute att = (Attribute) list.get(0);
            Double value = (Double) att.getValue();
            if (value == -1.0) return 0;
            return value;
        } catch (Exception e) {
            return 0;
        }
    }
    
    @Override
    public void dispose() {
        disposable.dispose();
    }
    
    @Override
    public boolean isDisposed() {
        return disposable.isDisposed();
    }
}
