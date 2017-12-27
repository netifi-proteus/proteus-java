package io.netifi.proteus.frames.admin;

/** */
public enum AdminFrameType {
  UNDEFINED(0x00),
  ADMIN_SETUP_FRAME(0x01),
  ADMIN_FRAME_PRESENCE_GROUP(0x02),
  ADMIN_FRAME_PRESENCE_DESTINATION(0x03),
  ADMIN_FRAME_LIST_GROUPS(0x04),
  ADMIN_FRAME_LIST_GROUPS_RESULTS(0x05),
  ADMIN_FRAME_TRACE(0x06),
  ADMIN_FRAME_ROUTER_NODE_INFO(0x07),
  ADMIN_FRAME_ROUTER_NODE_INFO_SNAPSHOT(0x08),
  ADMIN_FRAME_ROUTER_NODE_INFO_RESULT(0x09);

  private static AdminFrameType[] typesById;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (AdminFrameType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new AdminFrameType[max + 1];

    for (AdminFrameType t : values()) {
      typesById[t.id] = t;
    }
  }

  private final int id;
  private final int flags;

  AdminFrameType(final int id) {
    this(id, 0);
  }

  AdminFrameType(int id, int flags) {
    this.id = id;
    this.flags = flags;
  }

  public static AdminFrameType from(int id) {
    return typesById[id];
  }

  public int getEncodedType() {
    return id;
  }
}
