package io.netifi.proteus.frames.admin;

public enum AdminRouterNodeInfoEventType {
  UNDEFINED(0x00),
  // REQUEST REPLY
  JOIN(0x01),
  LEAVE(0x02);

  private static AdminRouterNodeInfoEventType[] typesById;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (AdminRouterNodeInfoEventType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new AdminRouterNodeInfoEventType[max + 1];

    for (AdminRouterNodeInfoEventType t : values()) {
      typesById[t.id] = t;
    }
  }

  private final int id;

  AdminRouterNodeInfoEventType(final int id) {
    this.id = id;
  }

  public static AdminRouterNodeInfoEventType from(int id) {
    return typesById[id];
  }

  public int getEncodedType() {
    return id;
  }
}
