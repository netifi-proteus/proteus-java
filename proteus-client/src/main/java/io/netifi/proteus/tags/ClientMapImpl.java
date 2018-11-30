package io.netifi.proteus.tags;

import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import io.netifi.proteus.tags.ClientMap.Event.EventType;
import java.util.*;
import java.util.Map.Entry;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

public class ClientMapImpl<V> implements ClientMap<V> {
  private final DirectProcessor<Event<V>> events;
  private final Long2ObjectHashMap<V> keyToValue;
  private final Long2ObjectHashMap<Tags> keyToTags;
  private final Table<CharSequence, CharSequence, RoaringLongBitmap> tagIndexes;

  public ClientMapImpl() {
    this.events = DirectProcessor.create();
    this.keyToValue = new Long2ObjectHashMap<>();
    this.keyToTags = new Long2ObjectHashMap<>();
    this.tagIndexes =
        Tables.newCustomTable(new Object2ObjectHashMap<>(), Object2ObjectHashMap::new);
  }

  @Override
  public List<V> query(Tags tags) {
    RoaringLongBitmap result = null;
    synchronized (this) {
      for (Entry<CharSequence, CharSequence> entry : tags) {
        RoaringLongBitmap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());

        if (bitmap == null) {
          return Collections.emptyList();
        }

        if (result == null) {
          result = new RoaringLongBitmap();
          result.or(bitmap);
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

    return new ClientNavigableMapList(result);
  }

  @Override
  public List<V> query(int brokerId, Tags tags) {
    RoaringBitmap result = null;
    synchronized (this) {
      for (Entry<CharSequence, CharSequence> entry : tags) {
        RoaringLongBitmap map = tagIndexes.get(entry.getKey(), entry.getValue());

        if (map == null) {
          return Collections.emptyList();
        }

        RoaringBitmap bitmap = map.getBitmap(brokerId);

        if (bitmap == null) {
          return Collections.emptyList();
        }

        if (result == null) {
          result = new RoaringBitmap();
          result.or(bitmap);
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

    return new RoaringBitmapList(brokerId, result);
  }

  @Override
  public Flux<Event<V>> events() {
    return events;
  }

  @Override
  public Flux<Event<V>> events(Tags tags) {
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
  public Flux<Event<V>> eventsByTagKeys(CharSequence... tags) {
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
        RoaringLongBitmap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());
        if (bitmap == null) {
          bitmap = new RoaringLongBitmap();
          tagIndexes.put(entry.getKey(), entry.getValue(), bitmap);
        }

        bitmap.add(keyIndex);
      }
    }

    Event<V> kEvent = Event.of(EventType.ADD, brokerId, clientId, value, tags);
    events.onNext(kEvent);

    return oldValue;
  }

  private void removeTags(long keyIndex, Tags tags) {
    Objects.requireNonNull(tags);
    for (Entry<CharSequence, CharSequence> entry : tags) {
      RoaringLongBitmap bitmap = tagIndexes.get(entry.getKey(), entry.getValue());

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
      Event<V> event;
      synchronized (this) {
        Tags tags = keyToTags.remove(keyIndex);
        if (tags != null) {
          removeTags(keyIndex, tags);
        }

        event = Event.of(EventType.REMOVE, brokerId, clientId, oldValue, tags);
      }

      events.onNext(event);
    }

    return oldValue;
  }

  private class ClientNavigableMapList extends AbstractList<V> {
    private final RoaringLongBitmap result;

    public ClientNavigableMapList(RoaringLongBitmap result) {
      this.result = result;
    }

    @Override
    public V get(int index) {
      return keyToValue.get(result.select(index));
    }

    @Override
    public Iterator<V> iterator() {
      return new ClientNavigableMapIterator(result.getLongIterator());
    }

    @Override
    public int size() {
      return result.getIntCardinality();
    }
  }

  private class ClientNavigableMapIterator implements Iterator<V> {
    private final LongIterator queryResults;

    ClientNavigableMapIterator(LongIterator queryResults) {
      this.queryResults = queryResults;
    }

    @Override
    public boolean hasNext() {
      return queryResults.hasNext();
    }

    @Override
    public V next() {
      return keyToValue.get(queryResults.next());
    }
  }

  private class RoaringBitmapList extends AbstractList<V> {
    private final int brokerId;
    private final RoaringBitmap result;

    public RoaringBitmapList(int brokerId, RoaringBitmap result) {
      this.brokerId = brokerId;
      this.result = result;
    }

    @Override
    public V get(int index) {
      int clientId = result.select(index);
      return keyToValue.get(Hashing.compoundKey(brokerId, clientId));
    }

    @Override
    public Iterator<V> iterator() {
      return new RoaringBitmapIterator(brokerId, result.getIntIterator());
    }

    @Override
    public int size() {
      return result.getCardinality();
    }
  }

  private class RoaringBitmapIterator implements Iterator<V> {
    private final int brokerId;
    private final IntIterator queryResults;

    RoaringBitmapIterator(int brokerId, IntIterator queryResults) {
      this.brokerId = brokerId;
      this.queryResults = queryResults;
    }

    @Override
    public boolean hasNext() {
      return queryResults.hasNext();
    }

    @Override
    public V next() {
      int clientId = queryResults.next();
      return keyToValue.get(Hashing.compoundKey(brokerId, clientId));
    }
  }
}
