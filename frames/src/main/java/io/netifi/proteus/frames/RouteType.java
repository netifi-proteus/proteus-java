package io.netifi.proteus.frames;

/** */
public enum RouteType {
  UNDEFINED(0x00, false),
  STREAM_ID_ROUTE(0x01, true),
  STREAM_GROUP_ROUTE(0x02, false),
  PRESENCE_ID_QUERY(0x03, true),
  PRESENCE_GROUP_QUERY(0x04, false);

  private static RouteType[] typesById;

  private final int id;
  private final boolean hasDestination;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (RouteType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new RouteType[max + 1];

    for (RouteType t : values()) {
      typesById[t.id] = t;
    }
  }

  RouteType(int id, boolean hasDestination) {
    this.id = id;
    this.hasDestination = hasDestination;
  }

  public int getEncodedType() {
    return id;
  }

  public boolean hasDestination() {
    return hasDestination;
  }

  public static RouteType from(int id) {
    return typesById[id];
  }
}
