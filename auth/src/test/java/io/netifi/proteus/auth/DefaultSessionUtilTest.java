package io.netifi.proteus.auth;

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;

/** */
public class DefaultSessionUtilTest {
  TestClock clock = new TestClock();
  DefaultSessionUtil sessionUtil = new DefaultSessionUtil(clock);

  @Test
  public void testDeepEquals() {
    byte[] a1 = "hello world!".getBytes();
    byte[] a2 = "hello world!".getBytes();
    byte[] a3 = "goodbye world!".getBytes();
    byte[] a4 = "hello world?".getBytes();
    boolean equals = deepEquals(a1, a2);
    Assert.assertTrue(equals);
    equals = deepEquals(a1, a1);
    Assert.assertTrue(equals);
    equals = deepEquals(a1, a3);
    Assert.assertFalse(equals);
    equals = deepEquals(a1, a4);
    Assert.assertFalse(equals);
    equals = deepEquals(a1, null);
    Assert.assertFalse(equals);
  }

  @Test
  public void testGetSteps() {
    clock.setTime(0);
    long thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(0, thirtySecondsStepsFromEpoch);
    clock.setTime(30000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(1, thirtySecondsStepsFromEpoch);
    clock.setTime(35000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(1, thirtySecondsStepsFromEpoch);
    clock.setTime(60000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(2, thirtySecondsStepsFromEpoch);
    clock.setTime(63000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(2, thirtySecondsStepsFromEpoch);
  }

  @Test
  public void testGetStepsAsByteArray() {
    clock.setTime(60000);
    byte[] stepsAsByteArray = sessionUtil.getStepsAsByteArray(2);
    byte[] bytes = new byte[8];
    ByteBuffer.wrap(bytes).putLong(2);
    Assert.assertArrayEquals(bytes, stepsAsByteArray);
  }

  @Test
  public void testGenerateToken() {
    clock.setTime(0);
    byte[] key = "super secret password".getBytes();
    byte[] message = "hello world!".getBytes();
    long epoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    byte[] m1 = sessionUtil.generateSessionToken(key, Unpooled.wrappedBuffer(message), epoch);

    byte[] m3 = sessionUtil.generateSessionToken(key, Unpooled.wrappedBuffer(message), epoch + 1);

    Assert.assertNotNull(m1);
    Assert.assertNotNull(m3);

    Assert.assertFalse(deepEquals(m1, m3));
  }

  @Test
  public void testGenerateRequestToken() {
    clock.setTime(0);
    byte[] key = "super secret password".getBytes();
    String destination = "test";
    long epoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    byte[] sessionToken =
        sessionUtil.generateSessionToken(
            key, Unpooled.wrappedBuffer(destination.getBytes()), epoch);

    int r1 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("a new request".getBytes()), epoch);

    clock.setTime(40000);
    int r2 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("a new request".getBytes()), epoch);
    Assert.assertEquals(r1, r2);
    clock.setTime(40000);

    int r3 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("another request".getBytes()), epoch + 1);
    Assert.assertNotEquals(r1, r3);

    clock.setTime(60000);
    int r4 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("a new request".getBytes()), epoch + 2);
    Assert.assertNotEquals(r1, r4);
  }

  @Test
  public void testValidateMessage() {
    clock.setTime(0);
    byte[] key = "super secret password".getBytes();
    String destination = "test";
    long epoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    byte[] sessionToken =
        sessionUtil.generateSessionToken(
            key, Unpooled.wrappedBuffer(destination.getBytes()), epoch);
    byte[] message = "a request".getBytes();

    int requestToken =
        sessionUtil.generateRequestToken(sessionToken, Unpooled.wrappedBuffer(message), epoch + 1);

    boolean valid =
        sessionUtil.validateMessage(
            sessionToken, Unpooled.wrappedBuffer(message), requestToken, epoch + 1);
    Assert.assertTrue(valid);

    clock.setTime(40000);
    valid =
        sessionUtil.validateMessage(
            sessionToken, Unpooled.wrappedBuffer(message), requestToken, epoch + 2);
    Assert.assertFalse(valid);
  }

  boolean deepEquals(byte[] a1, byte[] a2) {
    if (a1 == a2) return true;
    if (a1 == null || a2 == null) return false;
    int length = a1.length;
    if (a2.length != length) return false;

    for (int i = 0; i < length; i++) {
      byte e1 = a1[i];
      byte e2 = a2[i];
      if (e1 != e2) {
        return false;
      }
    }
    return true;
  }

  class TestClock implements Clock {
    long time;

    public long getTime() {
      return time;
    }

    public void setTime(long time) {
      this.time = time;
    }

    @Override
    public long getEpochTime() {
      return time;
    }
  }
}
