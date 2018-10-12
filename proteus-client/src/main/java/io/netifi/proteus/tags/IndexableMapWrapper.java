package io.netifi.proteus.tags;

import com.google.common.collect.ForwardingMap;
import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

public class IndexableMapWrapper<K, V> extends ForwardingMap<K, V> implements IndexableMap<K, V> {
  private final Map<K, V> delegate;
  private final DirectProcessor<Event<K>> events;
  private final Int2ObjectHashMap<K> indexToKey;
  private final Object2IntHashMap<K> keyToIndex;
  private final Object2ObjectHashMap<K, Tags> keyToTags;
  private final Object2ObjectHashMap<
          CharSequence, Object2ObjectHashMap<CharSequence, RoaringBitmap>>
      tagIndexes;
  AtomicIntegerFieldUpdater<IndexableMapWrapper> KEY_INDEX =
      AtomicIntegerFieldUpdater.newUpdater(IndexableMapWrapper.class, "keyIndex");
  volatile int keyIndex;

  public IndexableMapWrapper(Map<K, V> delegate) {
    this.delegate = delegate;
    this.events = DirectProcessor.create();
    this.tagIndexes = new Object2ObjectHashMap<>();
    this.indexToKey = new Int2ObjectHashMap<>();
    this.keyToTags = new Object2ObjectHashMap<>();
    this.keyToIndex = new Object2IntHashMap<>(-1);
  }

  /**
   * Wraps an map
   *
   * @param target the map to wrapped
   * @param <K>
   * @param <V>
   * @return a IndexableMap
   */
  public static <K, V> IndexableMapWrapper<K, V> wrap(Map<K, V> target) {
    return new IndexableMapWrapper<>(target);
  }

  @Override
  public boolean contains(Tags tags) {
    RoaringBitmap result = null;
    synchronized (this) {
      for (Entry<CharSequence, CharSequence> entry : tags) {
        Object2ObjectHashMap<CharSequence, RoaringBitmap> indexes = tagIndexes.get(entry.getKey());

        if (indexes == null) {
          return false;
        }

        RoaringBitmap bitmap = indexes.get(entry.getValue());

        if (bitmap == null) {
          return false;
        }

        if (result == null) {
          result = bitmap;
        } else {
          result.and(bitmap);
        }

        if (result.isEmpty()) {
          return false;
        }
      }
    }

    if (result == null) {
      return false;
    }

    return !result.isEmpty();
  }

  @Override
  public Iterable<Entry<K, V>> query(Tags tags) {
    RoaringBitmap result = null;
    synchronized (this) {
      for (Entry<CharSequence, CharSequence> entry : tags) {
        Object2ObjectHashMap<CharSequence, RoaringBitmap> indexes = tagIndexes.get(entry.getKey());

        if (indexes == null) {
          return Collections.emptyList();
        }

        RoaringBitmap bitmap = indexes.get(entry.getValue());

        if (bitmap == null) {
          return Collections.emptyList();
        }

        if (result == null) {
          result = bitmap;
        } else {
          result.and(bitmap);
        }

        if (result.isEmpty()) {
          return Collections.emptyList();
        }
      }
    }

    if (result == null) {
      return Collections.emptyList();
    }

    return new QueryResultIterable(result.getIntIterator());
  }

  private synchronized Entry<K, V> getEntry(int keyIndex) {
    K k = indexToKey.get(keyIndex);
    V v = delegate.get(k);
    return new SimpleImmutableEntry<>(k, v);
  }

  @Override
  public Flux<Event<K>> events() {
    return events;
  }

  @Override
  public Flux<Event<K>> events(Tags tags) {
    Objects.requireNonNull(tags);

    return events.filter(
        event -> {
          Tags eventTags = event.getTags();
          if (eventTags == null) {
            return false;
          }

          for (Entry<CharSequence, CharSequence> entry : tags) {
            if (!eventTags.contains(entry.getKey(), entry.getValue())) {
              return false;
            }
          }

          return true;
        });
  }

  @Override
  public Flux<Event<K>> eventsByTagKeys(CharSequence... tags) {
    Objects.requireNonNull(tags);

    return events.filter(
        event -> {
          Tags eventTags = event.getTags();
          if (eventTags == null) {
            return false;
          }

          for (CharSequence key : tags) {
            if (!eventTags.contains(key)) {
              return false;
            }
          }

          return true;
        });
  }

  @Override
  protected Map<K, V> delegate() {
    return delegate;
  }

  @Override
  public V put(K key, V value) {
    return put(key, value, EmptyTags.INSTANCE);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    standardPutAll(map);
  }

  @Override
  public V put(K key, V value, Tags tags) {
    synchronized (this) {
      int keyIndex = keyToIndex.getValue(key);
      if (keyIndex == keyToIndex.missingValue()) {
        keyIndex = getNextKeyIndex();
        indexToKey.put(keyIndex, key);
        keyToIndex.put(key, keyIndex);
        keyToTags.put(key, tags);
      } else {
        // Remove old tags
        Tags oldTags = keyToTags.put(key, tags);
        if (oldTags != null) {
          removeTags(keyIndex, oldTags);
        }
      }

      for (Entry<CharSequence, CharSequence> entry : tags) {
        RoaringBitmap bitmap =
            tagIndexes
                .computeIfAbsent(entry.getKey(), _k -> new Object2ObjectHashMap<>())
                .computeIfAbsent(entry.getValue(), _v -> new RoaringBitmap());

        bitmap.add(keyIndex);
      }
    }

    V oldValue = delegate.put(key, value);

    Event.EventType type = oldValue == null ? Event.EventType.ADD : Event.EventType.UPDATE;
    Event<K> kEvent = Event.of(type, key, tags);
    events.onNext(kEvent);

    return oldValue;
  }

  private void removeTags(int keyIndex, Tags tags) {
    Objects.requireNonNull(tags);
    for (Entry<CharSequence, CharSequence> entry : tags) {
      Object2ObjectHashMap<CharSequence, RoaringBitmap> bitmaps = tagIndexes.get(entry.getKey());

      if (bitmaps != null) {
        RoaringBitmap bitmap = bitmaps.get(entry.getValue());

        if (bitmap != null) {
          bitmap.remove(keyIndex);

          if (bitmap.isEmpty()) {
            bitmaps.remove(entry.getValue());
          }
        }

        if (bitmaps.isEmpty()) {
          tagIndexes.remove(entry.getKey());
        }
      }
    }
  }

  @Override
  public V remove(Object key) {
    V remove = delegate.remove(key);

    if (remove != null) {
      Event<K> event;
      synchronized (this) {
        int keyIndex = keyToIndex.removeKey((K) key);
        indexToKey.remove(keyIndex);

        Tags tags = keyToTags.remove(key);
        if (tags != null) {
          removeTags(keyIndex, tags);
        }

        event = Event.of(Event.EventType.REMOVE, (K) key, tags);
      }

      events.onNext(event);
    }

    return remove;
  }

  @Override
  public void clear() {
    standardClear();
  }

  private int getNextKeyIndex() {
    return KEY_INDEX.incrementAndGet(this);
  }

  private class QueryResultIterable implements Iterable<Entry<K, V>>, Iterator<Entry<K, V>> {
    private final PeekableIntIterator queryResults;

    QueryResultIterable(PeekableIntIterator queryResults) {
      this.queryResults = queryResults;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return queryResults.hasNext();
    }

    @Override
    public Entry<K, V> next() {
      if (queryResults.hasNext()) {
        int next = queryResults.next();
        return getEntry(next);
      } else {
        throw new NoSuchElementException();
      }
    }
  }
}
