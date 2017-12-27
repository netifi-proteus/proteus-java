package io.netifi.proteus.frames.admin;

/**
 * Different support trace types. There is a flag for each interaction model, and reactive streams
 * events so there isn't an extra byte being sent.
 */
public enum AdminTraceType {
  UNDEFINED(0x00),
  // REQUEST REPLY
  COMPLETE_REQUEST_REPLY(0x01),
  CANCEL_REQUEST_REPLY(0x02),
  ERROR_REQUEST_REPLY(0x03),

  // REQUEST STREAM
  COMPLETE_REQUEST_STREAM(0x04),
  CANCEL_REQUEST_STREAM(0x05),
  ERROR_REQUEST_STREAM(0x06),
  ON_NEXT_REQUEST_STREAM(0x07),

  // FIRE AND FORGET
  COMPLETE_FIRE_FORGET(0x08),
  CANCEL_FIRE_FORGET(0x09),
  ERROR_FIRE_FORGET(0x10),

  // METADATA PUSH
  COMPLETE_METADATA_PUSH(0x0A),
  CANCEL_METADATA_PUSH(0x0B),
  ERROR_METADATA_PUSH(0x0C),

  // REQUEST CHANNEL
  COMPLETE_REQUEST_CHANNEL(0x0D),
  CANCEL_REQUEST_CHANNEL(0x0E),
  ERROR_REQUEST_CHANNEL(0x0F),
  ON_NEXT_REQUEST_CHANEL(0x10);

  private static AdminTraceType[] typesById;

  /** Index types by id for indexed lookup. */
  static {
    int max = 0;

    for (AdminTraceType t : values()) {
      max = Math.max(t.id, max);
    }

    typesById = new AdminTraceType[max + 1];

    for (AdminTraceType t : values()) {
      typesById[t.id] = t;
    }
  }

  private final int id;

  AdminTraceType(final int id) {
    this.id = id;
  }

  public static AdminTraceType from(int id) {
    return typesById[id];
  }

  public int getEncodedType() {
    return id;
  }
}
