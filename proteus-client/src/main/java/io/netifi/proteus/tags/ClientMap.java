package io.netifi.proteus.tags;

import java.util.List;
import java.util.Objects;
import reactor.core.publisher.Flux;

public interface ClientMap<V> {
  V put(int brokerId, int clientId, V value, Tags tags);

  V remove(int brokerId, int clientId);

  List<V> query(Tags tags);

  List<V> query(int brokerId, Tags tags);

  Flux<Event<V>> events();

  Flux<Event<V>> events(Tags tags);

  Flux<Event<V>> eventsByTagKeys(CharSequence... tags);

  class Event<V> {
    private final EventType type;
    private final int brokerId;
    private final int clientId;
    private final V value;
    private final Tags tags;

    private Event(EventType type, int brokerId, int clientId, V value, Tags tags) {
      this.type = type;
      this.brokerId = brokerId;
      this.clientId = clientId;
      this.value = value;
      this.tags = tags;
    }

    public static <V> Event<V> of(EventType eventType, int brokerId, int clientId, V value, Tags tags) {
      return new Event<>(eventType, brokerId, clientId, value, tags);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Event event = (Event) o;
      return type == event.type && brokerId == event.brokerId && clientId == event.clientId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, brokerId, clientId);
    }

    public EventType getType() {
      return type;
    }

    public int getBrokerId() {
      return brokerId;
    }

    public int getClientId() {
      return clientId;
    }

    public V getValue() {
      return value;
    }

    public Tags getTags() {
      return tags;
    }

    public enum EventType {
      ADD,
      REMOVE
    }
  }
}
