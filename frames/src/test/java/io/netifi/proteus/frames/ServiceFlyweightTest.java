package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class ServiceFlyweightTest {
    @Test
    public void testEncodeAndDecode() {
        ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
        ByteBuf byteBuf =
            ServiceFlyweight.encode(
                ByteBufAllocator.DEFAULT, "汉字", "fromGroup", "service", metadata);
    
        Assert.assertEquals(FrameType.SERVICE, FrameHeaderFlyweight.frameType(byteBuf));
        Assert.assertEquals("汉字", ServiceFlyweight.fromDestination(byteBuf));
        Assert.assertEquals("fromGroup", ServiceFlyweight.fromGroup(byteBuf));
        Assert.assertEquals("service", ServiceFlyweight.service(byteBuf));
        metadata.resetReaderIndex();
        Assert.assertTrue(ByteBufUtil.equals(metadata, ServiceFlyweight.metadata(byteBuf)));
    }
}