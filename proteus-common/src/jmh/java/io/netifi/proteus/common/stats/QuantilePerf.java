package io.netifi.proteus.common.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class QuantilePerf {

  @Benchmark
  public double benchmarkInsert() {
    Ewma ewma = new Ewma(1, TimeUnit.SECONDS, 1.0);
    for (int i = 0; i < 100_000; i++) {
      ewma.insert(i);
    }

    return ewma.value();
  }

  @Benchmark
  public double benchmarkInsertFromThreads(Input input) throws Exception {
    List<? extends Callable<Double>> callables = input.callables;
    List<Future<Double>> futures = input.executor.invokeAll(callables);
    for (Future future : futures) {
      future.get();
    }
    return input.quantile.estimation();
  }

  @State(Scope.Thread)
  public static class Input {

    public int limit = 100_000;

    public int threads = 5;

    List<Callable<Double>> callables = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    Quantile quantile = new FrugalQuantile(4);

    @Setup
    public void setup() {
      for (int k = 0; k < threads; k++) {
        callables.add(
            () -> {
              for (int i = 0; i < limit; i++) {
                quantile.insert(i);
              }
              return quantile.estimation();
            });
      }
    }

    @TearDown
    public void teardown() {
      executor.shutdown();
    }
  }
}
