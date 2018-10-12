package io.netifi.proteus.tags;

import java.util.Map;
import java.util.Objects;
import reactor.core.publisher.Flux;

/**
 * A {@link Map} wrapper that lets you add tags to entries that a query via Bitmap indexes.
 * Additionally it will emit events when tags and entries are mutated. The wrapped class is only
 * thread-safe if the wrapped map is thread-safe. Once a map is wrapped it is not longer safe to
 * mutate the map. If you do so it could lead to corrupt indexes in the wrapper.
 *
 * @see Map
 * @author Robert Roeser
 */
public interface IndexableMap<K, V> extends Map<K, V> {
  V put(K key, V value, Tags tags);

  boolean contains(Tags tags);

  Iterable<Entry<K, V>> query(Tags tags);

  Flux<Event<K>> events();

  Flux<Event<K>> events(Tags tags);

  Flux<Event<K>> eventsByTagKeys(CharSequence... tags);

  class Event<K> {
    private final EventType type;
    private final K key;
    private final Tags tags;

    private Event(EventType type, K key, Tags tags) {
      this.type = type;
      this.key = key;
      this.tags = tags;
    }

    public static <K> Event<K> of(EventType eventType, K key, Tags tags) {
      return new Event<>(eventType, key, tags);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Event event = (Event) o;
      return type == event.type && Objects.equals(key, event.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, key);
    }

    public EventType getType() {
      return type;
    }

    public K getKey() {
      return key;
    }

    public Tags getTags() {
      return tags;
    }

    enum EventType {
      ADD,
      REMOVE,
      UPDATE
    }
  }
}
