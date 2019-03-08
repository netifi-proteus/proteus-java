package io.netifi.proteus.common.stats;

import io.netty.util.collection.LongObjectHashMap;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@State(Scope.Thread)
public class MapPerf {

  @State(Scope.Benchmark)
  public static class Context {

    private static final int ARRAY_SIZE = 1_000;

    public UUID[] testKeys;
    public Integer[] testValues;
    public Integer[] testValues2;
    public Random random;

    @Setup(Level.Trial)
    public void init() {
      random = new Random();
      testKeys = new UUID[ARRAY_SIZE];
      testValues = new Integer[ARRAY_SIZE];
      testValues2 = new Integer[ARRAY_SIZE];
      populateData();
    }

    @TearDown(Level.Trial)
    public void clean() {
      testKeys = null;
      testValues = null;
      random = null;
    }

    private void populateData() {
      for (int i = 0; i < testKeys.length; i++) {
        testKeys[i] = UUID.randomUUID();
        testValues[i] = random.nextInt();
        testValues2[i] = random.nextInt();
      }
    }
  }

  private void mapGet(
      Context context,
      Map<UUID, Integer>
          map) { // Populates a map, makes 50% successful and 50% unsuccessful get() calls
    // Populate the map
    for (int i = 0; i < context.testValues.length; i++)
      map.put(context.testKeys[i], context.testValues[i]);

    for (int i = 0; i < context.testValues.length / 2; i++) {
      map.get(context.testKeys[context.random.nextInt(context.testKeys.length)]); // Successful call
      map.get(UUID.randomUUID()); // Unsuccessful call
    }
  }

  private void mapPutUpdate(
      Context context, Map<UUID, Integer> map) { // Populates a map, then repopulates it
    // Populate the map the first time
    for (int i = 0; i < context.testValues.length; i++)
      map.put(context.testKeys[i], context.testValues[i]);

    // Update every map pair
    for (int i = 0; i < context.testValues.length; i++) {
      map.put(context.testKeys[i], context.testValues2[i]);
    }
  }

  private void mapPutRemove(
      Context context,
      Map<UUID, Integer> map) { // Populates a map, then manually clears the contents of it
    // Populate the map the first time
    for (int i = 0; i < context.testValues.length; i++)
      map.put(context.testKeys[i], context.testValues[i]);

    // Remove every map pair
    for (int i = 0; i < context.testValues.length; i++) {
      map.remove(context.testKeys[i]);
    }
  }

  // JDK Maps

  @Benchmark
  public void synchronizedHashMapGet(Context context) {
    Map<UUID, Integer> map = Collections.synchronizedMap(new HashMap<>());

    mapGet(context, map);
  }

  @Benchmark
  public void synchronizedHashMapPutUpdate(Context context) {
    Map<UUID, Integer> map = Collections.synchronizedMap(new HashMap<>());

    mapPutUpdate(context, map);
  }

  @Benchmark
  public void synchronizedHashMapPutRemove(Context context) {
    Map<UUID, Integer> map = Collections.synchronizedMap(new HashMap<>());

    mapPutRemove(context, map);
  }

  @Benchmark
  public void concurrentHashMapGet(Context context) {
    ConcurrentHashMap<UUID, Integer> map = new ConcurrentHashMap<>();

    mapGet(context, map);
  }

  @Benchmark
  public void concurrentHashMapPutUpdate(Context context) {
    ConcurrentHashMap<UUID, Integer> map = new ConcurrentHashMap<>();

    mapPutUpdate(context, map);
  }

  @Benchmark
  public void concurrentHashMapPutRemove(Context context) {
    ConcurrentHashMap<UUID, Integer> map = new ConcurrentHashMap<>();

    mapPutRemove(context, map);
  }

  // Netty

  @Benchmark
  public void nettyMapGet(Context context) {
    LongObjectHashMap<LongObjectHashMap<Integer>> map = new LongObjectHashMap<>();

    // Populate the map
    for (int i = 0; i < context.testValues.length; i++)
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new LongObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues[i]);

    for (int i = 0; i < context.testValues.length / 2; i++) {
      UUID successful = context.testKeys[context.random.nextInt(context.testKeys.length)];
      UUID unsuccessful = UUID.randomUUID();
      if (map.containsKey(successful.getMostSignificantBits())
          && map.get(successful.getMostSignificantBits())
              .containsKey(successful.getLeastSignificantBits())) {
        map.get(successful.getMostSignificantBits())
            .get(successful.getLeastSignificantBits()); // Successful call
      }
      if (map.containsKey(unsuccessful.getMostSignificantBits())
          && map.get(unsuccessful.getMostSignificantBits())
              .containsKey(unsuccessful.getLeastSignificantBits())) {
        map.get(unsuccessful.getMostSignificantBits())
            .get(unsuccessful.getLeastSignificantBits()); // Unsuccessful call
      }
    }
  }

  @Benchmark
  public void nettyMapPutUpdate(Context context) {
    LongObjectHashMap<LongObjectHashMap<Integer>> map = new LongObjectHashMap<>();

    // Populate the map the first time
    for (int i = 0; i < context.testValues.length; i++)
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new LongObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues[i]);

    // Update every map pair
    for (int i = 0; i < context.testValues.length; i++) {
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new LongObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues2[i]);
    }
  }

  @Benchmark
  public void nettyMapPutRemove(Context context) {
    LongObjectHashMap<LongObjectHashMap<Integer>> map = new LongObjectHashMap<>();

    // Populate the map the first time
    for (int i = 0; i < context.testValues.length; i++)
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new LongObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues[i]);

    // Remove every map pair
    for (int i = 0; i < context.testValues.length; i++) {
      if (map.containsKey(context.testKeys[i].getMostSignificantBits())) {
        LongObjectHashMap<Integer> submap = map.get(context.testKeys[i].getMostSignificantBits());
        submap.remove(context.testKeys[i].getLeastSignificantBits());
        if (submap.isEmpty()) {
          map.remove(context.testKeys[i].getMostSignificantBits());
        }
      }
    }
  }

  // Agrona

  @Benchmark
  public void agronaMapGet(Context context) {
    Long2ObjectHashMap<Long2ObjectHashMap<Integer>> map = new Long2ObjectHashMap<>();

    // Populate the map
    for (int i = 0; i < context.testValues.length; i++)
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new Long2ObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues[i]);

    for (int i = 0; i < context.testValues.length / 2; i++) {
      UUID successful = context.testKeys[context.random.nextInt(context.testKeys.length)];
      UUID unsuccessful = UUID.randomUUID();
      if (map.containsKey(successful.getMostSignificantBits())
          && map.get(successful.getMostSignificantBits())
              .containsKey(successful.getLeastSignificantBits())) {
        map.get(successful.getMostSignificantBits())
            .get(successful.getLeastSignificantBits()); // Successful call
      }
      if (map.containsKey(unsuccessful.getMostSignificantBits())
          && map.get(unsuccessful.getMostSignificantBits())
              .containsKey(unsuccessful.getLeastSignificantBits())) {
        map.get(unsuccessful.getMostSignificantBits())
            .get(unsuccessful.getLeastSignificantBits()); // Unsuccessful call
      }
    }
  }

  @Benchmark
  public void agronaMapPutUpdate(Context context) {
    Long2ObjectHashMap<Long2ObjectHashMap<Integer>> map = new Long2ObjectHashMap<>();

    // Populate the map the first time
    for (int i = 0; i < context.testValues.length; i++)
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new Long2ObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues[i]);

    // Update every map pair
    for (int i = 0; i < context.testValues.length; i++) {
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new Long2ObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues2[i]);
    }
  }

  @Benchmark
  public void agronaMapPutRemove(Context context) {
    Long2ObjectHashMap<Long2ObjectHashMap<Integer>> map = new Long2ObjectHashMap<>();

    // Populate the map the first time
    for (int i = 0; i < context.testValues.length; i++)
      map.computeIfAbsent(
              context.testKeys[i].getMostSignificantBits(), l -> new Long2ObjectHashMap<>())
          .put(context.testKeys[i].getLeastSignificantBits(), context.testValues[i]);

    // Remove every map pair
    for (int i = 0; i < context.testValues.length; i++) {
      if (map.containsKey(context.testKeys[i].getMostSignificantBits())) {
        Long2ObjectHashMap<Integer> submap = map.get(context.testKeys[i].getMostSignificantBits());
        submap.remove(context.testKeys[i].getLeastSignificantBits());
        if (submap.isEmpty()) {
          map.remove(context.testKeys[i].getMostSignificantBits());
        }
      }
    }
  }
}
