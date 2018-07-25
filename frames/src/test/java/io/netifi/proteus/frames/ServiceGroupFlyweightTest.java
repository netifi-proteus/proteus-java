package io.netifi.proteus.frames;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class ServiceGroupFlyweightTest {
    @Test
    public void testEncodeAndDecode() {
        ByteBuf metadata = Unpooled.wrappedBuffer("metadata".getBytes());
        ByteBuf byteBuf =
            ServiceGroupFlyweight.encode(
                ByteBufAllocator.DEFAULT, "汉字", "fromGroup", "service", "toGroup", metadata);
        
        Assert.assertEquals(FrameType.SERVICE_GROUP, FrameHeaderFlyweight.frameType(byteBuf));
        Assert.assertEquals("汉字", ServiceGroupFlyweight.fromDestination(byteBuf));
        Assert.assertEquals("fromGroup", ServiceGroupFlyweight.fromGroup(byteBuf));
        Assert.assertEquals("service", ServiceGroupFlyweight.service(byteBuf));
        Assert.assertEquals("toGroup", ServiceGroupFlyweight.toGroup(byteBuf));
        metadata.resetReaderIndex();
        Assert.assertTrue(ByteBufUtil.equals(metadata, ServiceGroupFlyweight.metadata(byteBuf)));
    }
}