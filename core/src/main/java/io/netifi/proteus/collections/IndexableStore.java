package io.netifi.proteus.collections;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;

public class IndexableStore<K, H extends IndexableStore.KeyHasher<K>, V> {
  private final Object2ObjectHashMap<QueryKey, List<Entry<V>>> queryCache;
  private final BiInt2ObjectMap<Entry<V>> entries;
  private final H hasher;
  private final Object2ObjectHashMap<String, Index> indexes;

  @SuppressWarnings("unused")
  private final Object LOCK = new Object();

  public IndexableStore(H hasher) {
    this.entries = new BiInt2ObjectMap<>();
    this.indexes = new Object2ObjectHashMap<>();
    this.hasher = hasher;
    this.queryCache = new Object2ObjectHashMap<>();
  }

  public void clear() {
    synchronized (LOCK) {
      queryCache.clear();
      entries.clear();
      indexes.clear();
    }
  }

  @SuppressWarnings("unchecked")
  public Iterable<Entry<V>> entries() {
    synchronized (LOCK) {
      Object[] values = entries.getValues();
      if (values.length > 0) {
        Entry<V>[] v = (Entry<V>[]) values;
        return Arrays.asList(v);
      } else {
        return Collections.EMPTY_LIST;
      }
    }
  }

  public Entry<V> put(V entry, K... keys) {
    synchronized (LOCK) {
      long hash = hasher.hash(keys);
      Entry<V> of = Entry.of(hash, entry, indexes, LOCK);
      entries.put(hash, of);
      queryCache.clear();
      return of;
    }
  }

  public Entry<V> get(K... keys) {
    synchronized (LOCK) {
      long hash = hasher.hash(keys);
      return entries.get(hash);
    }
  }

  public boolean containsKey(K... keys) {
    synchronized (LOCK) {
      long hash = hasher.hash(keys);
      return entries.get(hash) != null;
    }
  }

  public boolean containsTag(String tag, String value) {
    synchronized (LOCK) {
      QueryKey queryKey = QueryKey.of(tag, value);

      Index index = indexes.get(tag);

      if (index == null) {
        return false;
      } else {
        Roaring64NavigableMap map = index.bitmaps.get(value);

        if (map == null) {
          return false;
        } else {
          return !map.isEmpty();
        }
      }
    }
  }

  public boolean containsTags(String... parameters) {
    synchronized (LOCK) {
      Objects.requireNonNull(parameters, "parameters is null");
      int length = parameters.length;
      if (length == 0 || isOdd(length)) {
        throw new IllegalStateException("parameters must be greater than zero 0 and even");
      }

      for (int i = 0; i < length; ) {
        String tag = parameters[i++];
        String value = parameters[i++];

        if (!containsTag(tag, value)) {
          return false;
        }
      }

      return true;
    }
  }

  public Entry<V> remove(K... keys) {
    synchronized (LOCK) {
      long hash = hasher.hash(keys);
      Entry<V> entry = entries.remove(hash);
      if (entry == null) {
        return null;
      }

      List<String> tags = new ArrayList<>(entry.tags.keySet());

      for (String s : tags) {
        entry.remove(s);
      }

      for (QueryKey key : entry.cachedQueries.keySet()) {
        queryCache.remove(key);
      }

      return entry;
    }
  }

  public List<Entry<V>> removeByQuery(String... parameters) {
    List<Entry<V>> query;
    synchronized (LOCK) {
      query = query(parameters);
      for (Entry<V> entry : query) {
        long hash = entry.hash;
        entries.remove(hash);
        List<String> tags = new ArrayList<>(entry.tags.keySet());

        for (String s : tags) {
          entry.remove(s);
        }

        for (QueryKey key : entry.cachedQueries.keySet()) {
          queryCache.remove(key);
        }
      }
    }

    return query;
  }

  private List<Entry<V>> checkQueryCache(QueryKey of) {
    synchronized (LOCK) {
      List<Entry<V>> cached = queryCache.get(of);

      return cached == null ? Collections.emptyList() : cached;
    }
  }

  public List<Entry<V>> fromIndex(String name) {
    synchronized (LOCK) {
      Index index = indexes.get(name);
      if (index != null) {
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        for (Roaring64NavigableMap map : index.bitmaps.values()) {
          result.and(map);
        }

        if (result.isEmpty()) {
          return Collections.EMPTY_LIST;
        } else {
          List<Entry<V>> found = new ArrayList<>();
          Iterator<Long> iterator = result.iterator();
          while (iterator.hasNext()) {
            Entry<V> entry = entries.get(iterator.next());
            found.add(entry);
          }

          return found;
        }
      } else {
        return Collections.EMPTY_LIST;
      }
    }
  }

  public synchronized List<Entry<V>> query(String tag, String value) {
    synchronized (LOCK) {
      QueryKey queryKey = QueryKey.of(tag, value);
      List<Entry<V>> cached = checkQueryCache(queryKey);

      if (!cached.isEmpty()) {
        return cached;
      }

      Index index = indexes.get(tag);

      if (index == null) {
        return Collections.emptyList();
      } else {
        Roaring64NavigableMap map = index.bitmaps.get(value);

        if (map == null) {
          return Collections.emptyList();
        }

        List<Entry<V>> found = new ArrayList<>();
        Iterator<Long> iterator = map.iterator();
        while (iterator.hasNext()) {
          long hash = iterator.next();
          Entry<V> entry = this.entries.get(hash);
          entry.addQuery(queryKey);
          found.add(entry);
        }

        if (!found.isEmpty()) {
          queryCache.put(queryKey, found);
        }

        return found;
      }
    }
  }

  public synchronized List<Entry<V>> query(String... parameters) {
    synchronized (LOCK) {
      Objects.requireNonNull(parameters, "parameters is null");
      int length = parameters.length;
      if (length == 0 || isOdd(length)) {
        throw new IllegalStateException("parameters must be greater than zero 0 and even");
      }

      QueryKey queryKey = QueryKey.of(parameters);
      List<Entry<V>> cached = checkQueryCache(queryKey);

      if (!cached.isEmpty()) {
        return cached;
      }

      Roaring64NavigableMap result = null;
      for (int i = 0; i < length; ) {
        String tag = parameters[i++];
        String value = parameters[i++];

        Index index = indexes.get(tag);

        if (index == null) {
          return Collections.emptyList();
        }

        Roaring64NavigableMap map = index.bitmaps.get(value);

        if (map == null) {
          return Collections.emptyList();
        }

        if (result == null) {
          result = new Roaring64NavigableMap();
          result.or(map);
        } else {
          result.and(map);
        }
      }

      List<Entry<V>> found = new ArrayList<>();
      Iterator<Long> iterator = result.iterator();
      while (iterator.hasNext()) {
        long hash = iterator.next();
        Entry<V> entry = entries.get(hash);
        if (entry != null) {
          entry.addQuery(queryKey);
          found.add(entry);
        }
      }

      if (!found.isEmpty()) {
        queryCache.put(queryKey, found);
      }

      return found;
    }
  }

  private boolean isOdd(int number) {
    return 1 == (number & 1);
  }

  @FunctionalInterface
  public interface KeyHasher<T> {
    long hash(T... t);
  }

  public static class Entry<V> {
    private final long hash;
    private final Object2IntHashMap<QueryKey> cachedQueries;
    // tag -> value
    private final Object2ObjectHashMap<String, Set<String>> tags;
    // tag -> index (value -> bitmap)
    private final Object2ObjectHashMap<String, Index> indexes;
    private V entry;
    private Object LOCK;

    private Entry(long hash, V entry, Object2ObjectHashMap<String, Index> indexes, Object LOCK) {
      this.hash = hash;
      this.entry = entry;
      this.tags = new Object2ObjectHashMap<>();
      this.indexes = indexes;
      this.cachedQueries = new Object2IntHashMap<>(0);
      this.LOCK = LOCK;
    }

    static <V> Entry<V> of(
        long hash, V entry, Object2ObjectHashMap<String, Index> indexes, Object LOCK) {
      return new Entry<>(hash, entry, indexes, LOCK);
    }

    public Entry<V> add(String tag, String value) {
      synchronized (LOCK) {
        tags.computeIfAbsent(tag, t -> new HashSet<>()).add(value);
        Index index = indexes.computeIfAbsent(tag, Index::new);
        Roaring64NavigableMap map =
            index.bitmaps.computeIfAbsent(value, v -> new Roaring64NavigableMap());
        map.addLong(hash);

        for (QueryKey key : cachedQueries.keySet()) {
          cachedQueries.remove(key);
        }

        return this;
      }
    }

    public List<Entry<V>> remove(String tag) {
      List<Entry<V>> values = new ArrayList<>();
      synchronized (LOCK) {
        for (String value : tags.get(tag)) {
          Entry<V> remove = remove(tag, value);
          values.add(remove);
        }
      }

      return values;
    }

    public Entry<V> remove(String tag, String value) {
      synchronized (LOCK) {
        Index index = indexes.get(tag);
        if (index != null) {
          index.bitmaps.remove(value);

          if (index.bitmaps.isEmpty()) {
            indexes.remove(tag);
          }

          for (QueryKey key : cachedQueries.keySet()) {
            cachedQueries.remove(key);
          }
        }

        return this;
      }
    }

    void addQuery(QueryKey queryKey) {
      synchronized (LOCK) {
        cachedQueries.put(queryKey, 1);
      }
    }

    public V get() {
      synchronized (LOCK) {
        return entry;
      }
    }

    public void set(V v) {
      synchronized (LOCK) {
        entry = v;
      }
    }
  }

  private static class Index {
    private String name;
    private Object2ObjectHashMap<String, Roaring64NavigableMap> bitmaps;

    public Index(String name) {
      this.name = name;
      this.bitmaps = new Object2ObjectHashMap<>();
    }
  }

  private static class QueryKey {
    private String[] keys;

    private QueryKey(String... keys) {
      this.keys = keys;
    }

    public static QueryKey of(String... keys) {
      return new QueryKey(keys);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueryKey queryKey = (QueryKey) o;
      return Arrays.equals(keys, queryKey.keys);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(keys);
    }
  }
}
