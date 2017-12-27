package io.netifi.proteus.frames;

public enum RouterNodeInfoEventType {
  UNDEFINED(0x00),
  // REQUEST REPLY
  JOIN(0x01),
  LEAVE(0x02),
  SEED_NEXT(0x03),
  SEED_COMPLETE(0x04);

  private static RouterNodeInfoEventType[] typesById;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (RouterNodeInfoEventType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new RouterNodeInfoEventType[max + 1];

    for (RouterNodeInfoEventType t : values()) {
      typesById[t.id] = t;
    }
  }

  private final int id;

  RouterNodeInfoEventType(final int id) {
    this.id = id;
  }

  public static RouterNodeInfoEventType from(int id) {
    return typesById[id];
  }

  public int getEncodedType() {
    return id;
  }
}
