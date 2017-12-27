package io.netifi.proteus.frames;

/** */
public enum FrameType {
  UNDEFINED(0x00),
  DESTINATION_SETUP(0x01),
  ROUTER_SETUP(0x02),
  QUERY_SETUP(0x03),
  REQUEST_SHARED_SECRET(0x04),
  SHARED_SECRET(0x05),
  ROUTE(0x06),
  QUERY_DESTINATION_AVAIL(0x07),
  DESTINATION_AVAIL_RESULT(0x08),
  AUTH_REQUEST(0x09),
  AUTH_RESPONSE(0x0A),
  INFO_SETUP(0x10),
  ROUTER_INFO(0x11),
  ROUTER_INFO_SNAPSHOT(0x12),
  ROUTER_INFO_RESULT(0x13),
  EXTENSION_FRAME(0x7F);

  private static class Flags {
    private Flags() {}

    private static final int USER_DATA_PRESENT = 0b1000_0000;
    private static final int METADATA_PRESENT = 0b0100_0000;
    private static final int ENCRYPTED = 0b0010_0000;
    private static final int BROADCAST_MESSAGE = 0b0001_0000;
    private static final int API_CALL = 0b0000_1000;
    private static final int TOKEN = 0b0000_0100;
  }

  private static FrameType[] typesById;

  private final int id;
  private final int flags;

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

  FrameType(final int id) {
    this(id, 0);
  }

  FrameType(int id, int flags) {
    this.id = id;
    this.flags = flags;
  }

  public int getEncodedType() {
    return id;
  }

  public static FrameType from(int id) {
    return typesById[id];
  }
}
