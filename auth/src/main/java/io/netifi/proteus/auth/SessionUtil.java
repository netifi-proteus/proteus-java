package io.netifi.proteus.auth;

import io.netty.buffer.ByteBuf;

/** Generates a shared secret based on a string input. */
public abstract class SessionUtil {
  public static final SessionUtil instance() {
    return new DefaultSessionUtil();
  }

  public abstract byte[] generateSessionToken(byte[] key, ByteBuf data, long count);

  public abstract int generateRequestToken(byte[] sessionToken, ByteBuf message, long count);

  public abstract boolean validateMessage(
      byte[] sessionToken, ByteBuf message, int requestToken, long count);

  public abstract long getThirtySecondsStepsFromEpoch();
}
