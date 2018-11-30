package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class BrokerSetupFlyweightTest {
  @Test
  public void testEncoding() {
    ByteBuf authToken = Unpooled.wrappedBuffer("access token".getBytes());

    ByteBuf byteBuf =
        BrokerSetupFlyweight.encode(
            ByteBufAllocator.DEFAULT, 123, 456, Long.MAX_VALUE, authToken);

    Assert.assertEquals(123, BrokerSetupFlyweight.clusterId(byteBuf));
    Assert.assertEquals(456, BrokerSetupFlyweight.brokerId(byteBuf));
    Assert.assertEquals(Long.MAX_VALUE, BrokerSetupFlyweight.accessKey(byteBuf));
    authToken.resetReaderIndex();
    Assert.assertTrue(ByteBufUtil.equals(authToken, BrokerSetupFlyweight.accessToken(byteBuf)));
  }
}
