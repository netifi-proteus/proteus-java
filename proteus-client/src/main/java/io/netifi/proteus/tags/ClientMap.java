package io.netifi.proteus.tags;

import java.util.Map.Entry;
import java.util.Objects;
import reactor.core.publisher.Flux;

public interface ClientMap<V> {
  V put(int brokerId, int clientId, V value, Tags tags);

  V remove(int brokerId, int clientId);

  boolean contains(Tags tags);

  Iterable<Entry<Long, V>> query(Tags tags);

  Flux<Event> events();

  Flux<Event> events(Tags tags);

  Flux<Event> eventsByTagKeys(CharSequence... tags);

  class Event {
    private final EventType type;
    private final int brokerId;
    private final int clientId;
    private final Tags tags;

    private Event(EventType type, int brokerId, int clientId, Tags tags) {
      this.type = type;
      this.brokerId = brokerId;
      this.clientId = clientId;
      this.tags = tags;
    }

    public static Event of(EventType eventType, int brokerId, int clientId, Tags tags) {
      return new Event(eventType, brokerId, clientId, tags);
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

    public Tags getTags() {
      return tags;
    }

    enum EventType {
      ADD,
      REMOVE
    }
  }
}
