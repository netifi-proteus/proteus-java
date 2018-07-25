package io.netifi.proteus.frames;

/** */
public enum FrameType {
  UNDEFINED(0x00),
  BROKER_SETUP(0x01),
  DESTINATION_SETUP(0x02),
  DESTINATION(0x03),
  GROUP(0x04),
  BROADCAST(0x05),
  SHARD(0x06),
  SERVICE(0x07),
  SERVICE_GROUP(0x08),
  SERVICE_DESTINATION(0x09),
  BROADCAST_SERVICE(0x10),
  BROADCAST_SERVICE_GROUP(0x11);

  private static FrameType[] typesById;

  private final int id;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (FrameType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new FrameType[max + 1];

    for (FrameType t : values()) {
      typesById[t.id] = t;
    }
  }

  FrameType(int id) {
    this.id = id;
  }

  public int getEncodedType() {
    return id;
  }

  public static FrameType from(int id) {
    return typesById[id];
  }
}
