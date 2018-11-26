package io.netifi.proteus.tags;

import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import io.netifi.proteus.tags.ClientMap.Event.EventType;
import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

public class ClientMapImpl<V> implements ClientMap<V> {
  private final DirectProcessor<Event> events;
  private final Long2ObjectHashMap<V> keyToValue;
  private final Long2ObjectHashMap<Tags> keyToTags;
  private final Table<CharSequence, CharSequence, Roaring64NavigableMap> tagIndexes;

  public ClientMapImpl() {
    this.events = DirectProcessor.create();
    this.keyToValue = new Long2ObjectHashMap<>();
    this.keyToTags = new Long2ObjectHashMap<>();
    this.tagIndexes =
        Tables.newCustomTable(new Object2ObjectHashMap<>(), Object2ObjectHashMap::new);
  }

  @Override
  public boolean contains(Tags tags) {
    Roaring64NavigableMap result = null;
    synchronized (this) {
      for (Entry<CharSequence, CharSequence> entry : tags) {
        Roaring64NavigableMap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());

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
  public Iterable<Entry<Long, V>> query(Tags tags) {
    Roaring64NavigableMap result = null;
    synchronized (this) {
      for (Entry<CharSequence, CharSequence> entry : tags) {
        Roaring64NavigableMap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());

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

    return new QueryResultIterable(result.getLongIterator());
  }

  @Override
  public Flux<Event> events() {
    return events;
  }

  @Override
  public Flux<Event> events(Tags tags) {
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
  public Flux<Event> eventsByTagKeys(CharSequence... tags) {
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
  public V put(int brokerId, int clientId, V value, Tags tags) {
    long keyIndex = Hashing.compoundKey(brokerId, clientId);

    V oldValue;
    synchronized (this) {
      oldValue = keyToValue.put(keyIndex, value);
      keyToTags.put(keyIndex, tags);

      for (Entry<CharSequence, CharSequence> entry : tags) {
        Roaring64NavigableMap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());
        if (bitmap == null) {
          bitmap = new Roaring64NavigableMap();
          tagIndexes.put(entry.getKey(), entry.getValue(), bitmap);
        }

        bitmap.add(keyIndex);
      }
    }

    Event kEvent = Event.of(EventType.ADD, brokerId, clientId, tags);
    events.onNext(kEvent);

    return oldValue;
  }

  private void removeTags(long keyIndex, Tags tags) {
    Objects.requireNonNull(tags);
    for (Entry<CharSequence, CharSequence> entry : tags) {
      Roaring64NavigableMap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());

      if (bitmap != null) {
        bitmap.removeLong(keyIndex);

        if (bitmap.isEmpty()) {
          tagIndexes.remove(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  @Override
  public V remove(int brokerId, int clientId) {
    long keyIndex = Hashing.compoundKey(brokerId, clientId);
    V oldValue = keyToValue.remove(keyIndex);

    if (oldValue != null) {
      Event event;
      synchronized (this) {
        Tags tags = keyToTags.remove(keyIndex);
        if (tags != null) {
          removeTags(keyIndex, tags);
        }

        event = Event.of(EventType.REMOVE, brokerId, clientId, tags);
      }

      events.onNext(event);
    }

    return oldValue;
  }

  private class QueryResultIterable implements Iterable<Entry<Long, V>>, Iterator<Entry<Long, V>> {
    private final LongIterator queryResults;

    QueryResultIterable(LongIterator queryResults) {
      this.queryResults = queryResults;
    }

    @Override
    public Iterator<Entry<Long, V>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return queryResults.hasNext();
    }

    @Override
    public Entry<Long, V> next() {
      if (queryResults.hasNext()) {
        long key = queryResults.next();
        V value = keyToValue.get(key);
        return new SimpleImmutableEntry<>(key, value);
      } else {
        throw new NoSuchElementException();
      }
    }
  }
}
