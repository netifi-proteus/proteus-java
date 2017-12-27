package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Ignore;
import org.junit.Test;

/** */
@Ignore
public class AuthenticationResponseFlyweightTest {
  @Test
  public void testEncodeTrue() {
    int length = AuthenticationResponseFlyweight.computeLength();
    ByteBuf byteBuf = Unpooled.buffer(length);
    // int encodedLength = AuthenticationResponseFlyweight.encode(byteBuf, true, 0);
    // Assert.assertEquals(length, encodedLength);
    // Assert.assertTrue(AuthenticationResponseFlyweight.authenticated(byteBuf));
  }

  @Test
  public void testEncodeFalse() {
    int length = AuthenticationResponseFlyweight.computeLength();
    ByteBuf byteBuf = Unpooled.buffer(length);
    //  int encodedLength = AuthenticationResponseFlyweight.encode(byteBuf, false, 0);
    //   Assert.assertEquals(length, encodedLength);
    // Assert.assertFalse(AuthenticationResponseFlyweight.authenticated(byteBuf));
  }
}
